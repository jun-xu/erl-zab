%%% -------------------------------------------------------------------
%%% Author  : clues
%%% Description :
%%%
%%% Created : Mar 5, 2013
%%% -------------------------------------------------------------------
-module(zab_apply_server).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("zab.hrl").
-include("txnlog.hrl").
-include("log.hrl").
-include_lib("kernel/include/file.hrl").
%% --------------------------------------------------------------------
%% External exports
-export([start_link/0,get_last_commit_zxid/0,commit/2,stop/1,reset_callback/2,load_msg_to_commit/1,clear_callback/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {last_snap_zxid,
				fd,
				apply_mod,
				offset,            %% last_commit_zxid.log offset
				last_commit_zxid  %% last commited zxid
  }).		

%% ====================================================================
%% External functions
%% ====================================================================

start_link() ->
	gen_server:start_link({local,?MODULE}, ?MODULE, [],[]).

%% if Zxid less than lastCommitZxid which will be ignored
commit(Zxid,Msg) ->
	gen_server:cast(?MODULE, {commit,Zxid,Msg}).

%% load all msg between commitedZxid to Zxid and commit them.
load_msg_to_commit(Zxid) ->
	gen_server:cast(?MODULE, {load_msg_to_commit,Zxid}).
	

get_last_commit_zxid() ->
	gen_server:call(?MODULE, get_last_commit_zxid,infinity).

stop(Reason) ->
	gen_server:call(?MODULE,{stop,Reason}).

reset_callback(Mod,Fun) ->
	gen_server:call(?MODULE, {rest_callback,Mod,Fun},infinity).

clear_callback() ->
	gen_server:call(?MODULE, clear_callback,infinity).
%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init([]) ->
	ApplyMod = case zab_util:get_app_env(apply_mod, [], undefined) of
				  undefined ->
					  ?WARN_F("~p -- no apply fun ...~n",[?MODULE]),
					  undefined;
				  F -> F
			   end,
%% 	Dir = zab_util:get_app_env(snapshot_dir, [], "./"),
	{FD,LastCommitZxid,Offset} = load_last_commit(),
	?INFO_F("~p -- start with apply mod:~p with last commit zxid:~p~n",[?MODULE,ApplyMod,LastCommitZxid]),
    {ok, #state{fd=FD,offset=Offset,last_commit_zxid=LastCommitZxid,apply_mod=ApplyMod}}.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------

handle_call({rest_callback,Mod,Fun}, _From, State) ->
	?INFO_F("~p -- reset apply mod:~p~n",[?MODULE,{Mod,Fun}]),
    {reply, ok, State#state{apply_mod={Mod,Fun}}};

handle_call(clear_callback, _From, State) ->
	?INFO_F("~p -- clear_callback apply mod.~n",[?MODULE]),
    {reply, ok, State#state{apply_mod=undefined}};


handle_call(get_last_commit_zxid, _From, #state{last_commit_zxid=Zxid}=State) ->
    {reply, {ok,Zxid}, State};

handle_call({stop,Reason}, _From,#state{fd=FD} = State) ->
	?INFO_F("~p -- stop by reason:~p~n",[?MODULE,Reason]),
	prim_file:close(FD),
	{stop, normal, ok, State};

handle_call(Request, _From, State) ->
	?INFO_F("~p -- not implement call:~p~n",[?MODULE,Request]),
    {reply, ok, State}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast({commit,Zxid,_}, #state{last_commit_zxid=LastZxid} = State) when Zxid =< LastZxid ->
	?INFO_F("~p -- already commit zxid:~p.~n",[?MODULE,Zxid]),
    {noreply, State};

handle_cast({commit,Zxid,_}, #state{apply_mod=undefined,fd=Fd,offset=Offset} = State) ->
	?WARN_F("~p -- ignore zxid:~p msg by reason:~p~n",[?MODULE,Zxid,no_apply_function]),
	{NewFd,NewOffset} = dump_to_file(Zxid, Fd, Offset),
	{noreply, State#state{last_commit_zxid=Zxid,fd=NewFd,offset=NewOffset}};

handle_cast({commit,Zxid,Msg}, #state{apply_mod={M,F},fd=Fd,offset=Offset} = State) -> %%when Counter < ?MAX_APPLY_LEN ->
	case commit_msg(Zxid,M,F,Msg) of
		ok ->
			{NewFd,NewOffset} = dump_to_file(Zxid, Fd, Offset),
			{noreply, State#state{last_commit_zxid=Zxid,fd=NewFd,offset=NewOffset}};
		E ->
			?INFO_F("~p -- commit msg:~p error:~p, stop.~n",[?MODULE,Zxid,E]),
			{stop, normal, State}    
	end;

handle_cast({load_msg_to_commit,Zxid}, #state{last_commit_zxid=LastZxid} = State) when Zxid =< LastZxid ->
	?INFO_F("~p -- already commit:~p~n",[?MODULE,Zxid]),
    {noreply, State};
handle_cast({load_msg_to_commit,Zxid}, #state{last_commit_zxid=CommitedId,apply_mod=undefined,fd=Fd,offset=Offset} = State) ->
	?WARN_F("~p -- ignore zxid:~p msg by reason:~p~n",[?MODULE,Zxid,no_apply_function]),
	case file_txn_log:load_logs(CommitedId) of
		{error,E} ->
			?WARN_F("~p -- load msgs to commit between ~p to ~p error:~p when applymod is undefined.~n",[?MODULE,CommitedId,Zxid,E]),
			{noreply, State};
		{ok,[]} ->
			?WARN_F("~p -- no msgs to commit between ~p to ~p when applymod is undefined.~n",[?MODULE,CommitedId,Zxid]),
			{noreply, State};
		{ok,Msgs} ->
			[{LastTxnLogZxid,_}|_] = lists:reverse(Msgs),
			{NewFd,NewOffset} = dump_to_file(LastTxnLogZxid, Fd, Offset),
    		{noreply, State#state{fd=NewFd,last_commit_zxid=LastTxnLogZxid,offset=NewOffset}}
	end;
	

handle_cast({load_msg_to_commit,Zxid}, #state{last_commit_zxid=CommitedId,apply_mod={M,F},fd=Fd,offset=Offset} = State) ->
	case file_txn_log:load_logs(CommitedId) of
		{error,E} ->
			?WARN_F("~p -- load msgs to commit between ~p to ~p ~nerror:~p.~n",[?MODULE,CommitedId,Zxid,E]),
			{noreply, State};
		{ok,[]} ->
			?WARN_F("~p -- no msgs to commit between ~p to ~p.~n",[?MODULE,CommitedId,Zxid]),
			{noreply, State};
		{ok,Msgs} ->
%% 			?DEBUG_F("~p -- load and commit:~p Zxid:~p~n",[?MODULE,Msgs,Zxid]),	
		  	case loop_commit_msgs(Msgs,M,F,Zxid,undefined) of
%% 				{ok,undefined} ->
%% 					?WARN_F("~p -- no msgs to commit successful between ~p to ~p.~n",[?MODULE,CommitedId,Zxid]),
%% 					{noreply, State};
				{ok,LastCommitZxid} ->
					{NewFd,NewOffset} = dump_to_file(LastCommitZxid, Fd, Offset),
					{noreply, State#state{fd=NewFd,last_commit_zxid=LastCommitZxid,offset=NewOffset}};
				{error,_E,undefined} ->
					?INFO_F("~p -- load commit msgs between ~p to ~p~n... stop.~n",[?MODULE,CommitedId,Zxid]),
					{stop, normal,State};
				{error,_E,LastCommitId} ->
					?DEBUG_F("~p -- dump to file:~p~n",[?MODULE,{LastCommitId, Fd, Offset}]),
					{NewFd,NewOffset} = dump_to_file(LastCommitId, Fd, Offset),
					?INFO_F("~p -- load commit msgs between ~p to ~p error with lastCommitedZxid:~p... stop.~n",[?MODULE,CommitedId,Zxid,LastCommitId]),
					{stop, normal, State#state{fd=NewFd,last_commit_zxid=LastCommitId,offset=NewOffset}}
					
			end
	end;
	

handle_cast(Msg, State) ->
	?INFO_F("~p -- not implement cast:~p~n",[?MODULE,Msg]),
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info(Info, State) ->
	?INFO_F("~p -- not implement info:~p~n",[?MODULE,Info]),
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(Reason, State) ->
	?INFO_F("~p -- terminate by reason:~p~n",[?MODULE,Reason]),
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(OldVsn, State, Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------


-spec load_last_commit() -> {FD::any(),LastCommitZxid::integer(),Offset::integer()}.
load_last_commit() ->
	{ok,FD} = prim_file:open(?LAST_COMMIT_ZXID_FILE_NAME,[read,write,binary,append]),
	case prim_file:read_file_info(?LAST_COMMIT_ZXID_FILE_NAME) of
		{ok,#file_info{size = 0}} ->
			{FD,0,0};
		{ok,#file_info{size = Size}} ->
			prim_file:position(FD, Size - 8),
			{ok,<<LastCommitZxid:64>>} = prim_file:read(FD, 8),
			{FD,LastCommitZxid,Size}
	end.

close_and_create_new_file(Fd) ->
	prim_file:close(Fd),
	prim_file:delete(?LAST_COMMIT_ZXID_FILE_NAME),
	prim_file:open(?LAST_COMMIT_ZXID_FILE_NAME,[write,binary,append]).


commit_msg(Zxid,M,F,Msg) ->
%% 	?DEBUG_F("~p -- commit zxid:~p~n",[?MODULE,Zxid]),
	case catch M:F(binary_to_term(Msg)) of
		{error,Reason} ->
			?ERROR_F("~p -- zxid:~p apply with error:~p",[?MODULE,Zxid,Reason]),
%% 			{error,apply_msg_failed};
			ok;
		{'EXIT',Pid,Reason} ->
			?ERROR_F("~p -- zxid:~p apply with error:~p",[?MODULE,Zxid,{Pid,Reason}]),
			{error,apply_msg_failed};
		{'EXIT',Reason} ->
			?ERROR_F("~p -- zxid:~p apply with msg:~p~nerror:~p",[?MODULE,Zxid,Msg,Reason]),
			{error,apply_msg_failed};
		_ ->
			ok
	end.

%% return when uncommitedzxis greater then zxid;
-spec loop_commit_msgs([{UncommitedZxid::integer(),Msg::binary()}],_,_,Zxid::integer(),LastCommitedZxid::integer()) ->
		  {ok,LastCommitedZxid::integer()} | {error,E::{error,Reason::any()},LastCommitedZxid::integer()}.
loop_commit_msgs([{UncommitedZxid,_Msg}|_],_M,_F,Zxid,LastCommitedZxid) when UncommitedZxid > Zxid-> 
	{ok,LastCommitedZxid};
%% commit msg and return when is last zxid msg.
loop_commit_msgs([{Zxid,Msg}|_],M,F,Zxid,LastCommitedZxid) ->
	case commit_msg(Zxid, M, F, Msg) of
		ok -> {ok,Zxid};
		E -> {error,E,LastCommitedZxid}
	end;
%% loop commit msg when uncommitedzxid lesser then zxid
loop_commit_msgs([{UncommitedZxid,Msg}|T],M,F,Zxid,LastCommitedZxid) ->
	case commit_msg(UncommitedZxid, M, F, Msg) of
		ok ->
			loop_commit_msgs(T, M, F, Zxid,UncommitedZxid);
		E -> {error,E,LastCommitedZxid}
	end.


dump_to_file(Zxid,Fd,Offset) ->
	case Offset >= ?COMMIT_ZXID_SIZE_PER_PAGE of
		true ->
			{ok,NewFd} = close_and_create_new_file(Fd),
			prim_file:write(NewFd, <<Zxid:64/integer>>),
			{NewFd,8};
		false ->
			prim_file:write(Fd, <<Zxid:64/integer>>),
    		{Fd,Offset+8}
	end.