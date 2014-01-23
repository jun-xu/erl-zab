%%% -------------------------------------------------------------------
%%% Author  : sunshine
%%% Description :
%%%
%%% Created : 2014-1-15
%%% -------------------------------------------------------------------
-module(file_txn_log_ex).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("log.hrl").
-include("txnlog_ex.hrl").
-include_lib("kernel/include/file.hrl").
%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------
-export([start_link/0,
		 stop/1,append/2,
		 get_last_zxid/0,
		 truncate_last_zxid/0,load_logs/2,
		 load_logs/1,get_last_msg/0,
		 get_value/1]).

%% --------------------------------------------------------------------
%% gen_server callbacks
%% --------------------------------------------------------------------
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(txn_log, {dir,								%% txnlog root dir.
				  current_file,						%% current txn log file FD.
				  current_file_data_offset = 0,
				  last_zxid,
				  last_msg
  }).

%% ====================================================================
%% External functions
%% ====================================================================
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec append(Zxid::integer(),Data::binary()) -> ok | {error,ignored}.
append(Zxid,Data) ->
	gen_server:call(?MODULE,{append,Zxid,Data},infinity).

-spec get_last_zxid() -> {ok,Zxid::integer()}.
get_last_zxid() ->
	gen_server:call(?MODULE,get_last_zxid,infinity).

get_last_msg() ->
	gen_server:call(?MODULE,get_last_msg,infinity).

-spec get_value(Zxid::integer()) ->{ok,Value::binary()}|{error,Reason::term()}.
get_value(Zxid) ->
	gen_server:call(?MODULE, {get_value,Zxid},infinity).

-spec truncate_last_zxid() -> ok.
truncate_last_zxid() ->
	gen_server:call(?MODULE, truncate_last_zxid,infinity).

%% binary data start from StartZxid+1 to EndZxid,
%% if EndZxid undefined,will replace with LastZxid
-spec load_logs(StartZxid::integer()) ->{ok,[{Zxid::integer(),Msg::binary()}]} | {error,Reason::term()}.
load_logs(StartZxid) ->
	gen_server:call(?MODULE, {load_log,StartZxid,all},infinity).

load_logs(StartZxid,LoadCount) ->
	gen_server:call(?MODULE, {load_log,StartZxid,LoadCount},infinity).

stop(Reason) ->
	gen_server:call(?MODULE,{stop,Reason}).
%%----------------------------------------------------------------------
%% @spec init(Args) -> {ok, State}           |
%%                            {ok, State, Timeout}  |
%%                            ignore                |
%%                            {stop, Reason}
%%
%% @doc
%%      Initializes the server
%% @end
%%----------------------------------------------------------------------
init([]) ->
	Dir = zab_util:get_app_env(txn_log_dir, [], "./"),
	?INFO_F("~p -- start txn log with dir:~p~n",[?MODULE,Dir]),
	ok = filelib:ensure_dir(Dir),
	{ok,FD,Offset,LastZxid,LastMsg} = open_last_file(Dir),
    {ok, #txn_log{current_file_data_offset=Offset,dir=Dir,current_file=FD,last_zxid=LastZxid,last_msg=LastMsg}}.

%%--------------------------------------------------------------------
%% @private
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @doc
%% 		Handling call messages
%% @end
%%--------------------------------------------------------------------
%% normally, should not append data which zxid < lastzxid.
handle_call({append,Zxid,_Data}, _From, #txn_log{last_zxid=LastZxid} = State) when Zxid =< LastZxid ->
	?INFO_F("~p -- already append zxid:~p~n",[?MODULE,Zxid]),
	{reply, {error,ignored}, State};
handle_call({append,Zxid,Data}, _From, #txn_log{current_file=FD,dir=Dir,current_file_data_offset=DataOffset} = State) ->
	Length = byte_size(Data) + ?SIZE_OF_DATA_HEADER + 4,
	{NewFD,NewDataOffset} = case (DataOffset + Length) > ?ALLOC_SIZE_PER_PAGE of
											   true ->
												    %% roll log when data is overflow.
											   		roll_log(Dir,FD,Zxid);
											   false ->
												   	{FD,DataOffset}
											end,
	{ok,DataOffset1} = append_data_to_file(NewFD, NewDataOffset, Zxid, Data),
    {reply, ok, State#txn_log{current_file=NewFD,current_file_data_offset=DataOffset1,last_zxid=Zxid,last_msg=Data}};

handle_call(truncate_last_zxid, _From,#txn_log{last_zxid=?UNDEFINED_ZXID} = State) ->
	{reply, ok, State};
handle_call(truncate_last_zxid, _From,#txn_log{current_file=FD,dir=Dir,current_file_data_offset=DataOffset} = State) ->
	{ok,_} = prim_file:position(FD,DataOffset - 4),
	{ok,<<LastZxidOffset:32>>} = prim_file:read(FD, 4),
	case LastZxidOffset of
		0 -> 
			{ok,_} = prim_file:position(FD,0),
			ok = prim_file:truncate(FD),
			prim_file:close(FD),
			{ok,NewFD,Offset,LastZxid,LastMsg} = open_last_file(Dir),
			{reply, ok, State#txn_log{current_file=NewFD,current_file_data_offset=Offset,last_zxid=LastZxid,last_msg=LastMsg}};
		_ ->
			{ok,_} = prim_file:position(FD,LastZxidOffset - 4),
			{ok,<<PreLastZxidOffset:32>>} = prim_file:read(FD, 4),
			ok = prim_file:truncate(FD),
			{ok,Total} = prim_file:position(FD,{cur,0}),
			{ok,_} = prim_file:position(FD,PreLastZxidOffset),
			{ok,<<?TXN_LOG_FLAG,PreLastZxid:64,Length:32,_Crc:32,Msg:Length/binary,_:32>>} = prim_file:read(FD, Total - PreLastZxidOffset),
			{reply, ok, State#txn_log{current_file_data_offset=Total,last_zxid=PreLastZxid,last_msg=Msg}}
	end;


handle_call({stop,Reason}, _From,#txn_log{current_file=FD} = State) ->
	?INFO_F("~p -- stop by reason:~p~n",[?MODULE,Reason]),
	prim_file:close(FD),
	{stop, normal, ok, State};

handle_call(get_last_zxid, _From,#txn_log{last_zxid=Zxid} = State) ->
    {reply, {ok,Zxid}, State};

handle_call(get_last_msg,_,#txn_log{last_zxid=Zxid,last_msg=Msg} = State) ->
    {reply, {ok,{Zxid,Msg}}, State};

handle_call({get_value,Zxid}, _From,#txn_log{dir=Dir}=State) ->
	Reply = case read_zxid(Dir,Zxid) of
				{ok,Val} ->
					{ok,Val};
				Error ->
					Error
			end,
    {reply, Reply, State};


handle_call({load_log,StartZxid,_LoadCount}, _From,#txn_log{last_zxid=LastZxid} = State) when StartZxid > LastZxid->
	{reply, {error,not_found}, State};
handle_call({load_log,LastZxid,_LoadCount}, _From,#txn_log{last_zxid=LastZxid} = State) ->
	{reply, {ok,[]}, State};
handle_call({load_log,StartZxid,LoadCount}, _From,#txn_log{dir=Dir} = State) ->
    Reply = load_logs0(Dir,StartZxid,LoadCount),
	{reply, Reply, State};

handle_call(Request, From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @doc
%% 		Handling cast messages
%% @end
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @doc
%% 		Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
handle_info(Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @spec terminate(Reason, State) -> void()
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
terminate(Reason, #txn_log{current_file=FD}) ->
	case FD of
		undefined -> ok;
		_ -> 
			prim_file:close(FD),
			ok
	end,
	?WARN_F("~p -- terminate by reason:~p~n",[?MODULE,Reason]),
    ok.
%%-------------------------------------------------------------------------
%% @private
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc  Convert process state when code is changed.
%% @end
%%-------------------------------------------------------------------------
code_change(OldVsn, State, Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------
%% filter logs with name which start with "log."
open_last_file(Dir) ->
	Files = filelib:fold_files(Dir, "log.[0-9]+", false,
                               fun(F, Acc) ->
									case get_base_name(F) of
										V ->
											[V|Acc]
									end
                               end, []
					  ),
	open_last_file0(Dir,Files).
	
%% if txn log dir is empty,create new.
open_last_file0(Dir,[]) ->
	{ok,FD} = create_new_file(Dir,0),
	{ok,FD,0,?UNDEFINED_ZXID,undefined};
%% otherwise,open the biggest zxid file.
open_last_file0(Dir,Files) ->
	[{_,LastFile}|T] = lists:reverse(lists:sort(Files)),
	case open_file(LastFile) of
		{error,R} ->
			?INFO_F("~p -- load txn file:~p error by reason:~p delete.",[?MODULE,LastFile,R]),
			prim_file:delete(LastFile),
			open_last_file0(Dir,T);
		{ok,_,_,_,_} = Info ->
			Info
	end.

create_new_file(Dir,Zxid) ->
	prim_file:open(filename:join([Dir,lists:concat(["log.",Zxid])]),?FILE_WRITE_OPEN_OPTIONS).

get_base_name(FileName) ->
	{match,[ZxidStr]} =  re:run(filename:basename(FileName),"^log.([0-9]+)$",[{capture,all_but_first,list}]),
	{list_to_integer(ZxidStr),FileName}.

-spec open_file(FileName::string()) -> {ok,{FD::any(),LastZxid::integer(),Msg::binary()}}.
open_file(FileName) ->
	{ok,FD} = prim_file:open(FileName,?FILE_WRITE_OPEN_OPTIONS),
	case loop_analyze_txnlog(FD,?DEFAULT_LOAD_FILE_CHUNK_SIZE,undefined,<<>>) of
		{ok,_,undefined} -> 
			prim_file:close(FD),
			{error,invalide_file};
		{ok,Offset,{Zxid,LastMsg}} ->
			{ok,FD,Offset,Zxid,LastMsg}
	end.

loop_analyze_txnlog(FD,LoadSize,LastMsgInfo,B) ->
	case prim_file:read(FD, LoadSize) of
		eof -> 
			{ok,Pos} = prim_file:position(FD, {cur,0}),
			case B of
				<<>> -> {ok,Pos,LastMsgInfo};
				_ ->
					NewPos = Pos-byte_size(B),
					{ok,_} = prim_file:position(FD, NewPos),
					ok = prim_file:truncate(FD),
					{ok,NewPos,LastMsgInfo}
			end;
		{ok,Data} ->
			{ok,LastMsgInfo0,RestBin} = loop_analyze_txnlog0(<<B/binary,Data/binary>>,undefined),
			loop_analyze_txnlog(FD, LoadSize, LastMsgInfo0, RestBin)
	end.
	
loop_analyze_txnlog0(<<?TXN_LOG_FLAG,Zxid:64,Length:32,_Crc:32,LastMsg:Length/binary,_totalSize:32>>,_) ->
	{ok,{Zxid,LastMsg},<<>>};
loop_analyze_txnlog0(<<?TXN_LOG_FLAG,Zxid:64,Length:32,_Crc:32,LastMsg:Length/binary,_totalSize:32,Rest/binary>>,_) ->
	loop_analyze_txnlog0(Rest,{Zxid,LastMsg});
loop_analyze_txnlog0(B, LastMsgInfo) -> {ok,LastMsgInfo,B}. 
	

roll_log(Dir,FD,Zxid) ->
	prim_file:close(FD),
	{ok,NewFD} = create_new_file(Dir,Zxid),
	{NewFD,0}.
	

append_data_to_file(FD,DataOffset,Zxid,Data) ->
	Crc = erlang:crc32(Data),
	Length = size(Data),
	B = <<?TXN_LOG_FLAG,Zxid:64,Length:32,Crc:32,Data/binary,DataOffset:32>>,
	ok = prim_file:write(FD,B),
	{ok,DataOffset+size(B)}.
	
load_logs0(Dir,Zxid,TotalAccount) ->
	{ok,Files} = fiter_all_file_by(Dir, Zxid),
	loop_load_msgs_of_log(Zxid,Files, [], TotalAccount).

fiter_all_file_by(Dir,Zxid) ->
	Fun = fun(Fullname,AccIn) ->
				  Ext = filename:extension(Fullname),
				  Num = list_to_integer(tl(Ext)),
				  [{Num,Fullname}|AccIn]
		  end,
	AllFiles = filelib:fold_files(Dir, "log\.[0-9]+$", false, Fun, []),
	{ok,Files} = filter_file(Zxid,lists:sort(AllFiles),[],undefined),
	{ok,Files}.

read_zxid(Dir,Zxid) ->
	{ok,Files} = fiter_all_file_by(Dir, Zxid),
	loop_find_msgs_of_log(Zxid,Files).
	

filter_file(_Zxid,[],L,undefined) -> {ok,lists:reverse(L)};
filter_file(_Zxid,[],L,File) -> {ok,lists:reverse([File|L])};
filter_file(Zxid,[{Num,File}|T],L,_) when Num =< Zxid -> 
	filter_file(Zxid,T,L,File);
filter_file(Zxid,[{_,File}|T],L,undefined) ->
	filter_file(Zxid, T, [File|L], undefined);
filter_file(Zxid,[{_,File}|T],L,PreFile) ->
	filter_file(Zxid, T, [File,PreFile|L], undefined).
	
loop_load_msgs_of_log(_Zxid,_,{error,_}=E,_) -> E;
loop_load_msgs_of_log(_Zxid,[],L,_) -> {ok,lists:reverse(L)};
loop_load_msgs_of_log(_Zxid,_,L,0) -> {ok,lists:reverse(L)};
loop_load_msgs_of_log(Zxid,[File|T],L,Account) ->
	case load_from_file(Zxid,File,L,Account) of
		{ok,L1,RemainAccount} ->
			loop_load_msgs_of_log(Zxid,T,L1,RemainAccount);
		E -> E
	end.

loop_find_msgs_of_log(Zxid,[]) -> {error,not_found};
loop_find_msgs_of_log(Zxid,[File|T]) ->
	{ok,FD} = prim_file:open(File,[read,binary]),
	case loop_find_txnlog_file0(Zxid,FD,<<>>) of
		{error,not_found} ->
			prim_file:close(FD),
			loop_find_msgs_of_log(Zxid, T);
		{ok,V} ->
			prim_file:close(FD),
			{ok,V};
		{error,R} ->
			{error,R}
	end.

loop_find_txnlog_file0(Zxid,FD,B) ->
	case prim_file:read(FD, ?DEFAULT_LOAD_FILE_CHUNK_SIZE) of
		eof -> 
			{error,not_found};
		{ok,Data} ->
			case loop_find_txnlog_msgs(Zxid,<<B/binary,Data/binary>>) of
				{ok,V} -> {ok,V};
				{not_found,Rest} ->
					loop_find_txnlog_file0(Zxid, FD, Rest)
			end
	end.

loop_find_txnlog_msgs(Zxid,<<?TXN_LOG_FLAG,Zxid:64,Length:32,_Crc:32,Msg:Length/binary,_/binary>>) ->
	{ok,Msg};
loop_find_txnlog_msgs(Zxid,<<?TXN_LOG_FLAG,_NZxid:64,Length:32,_Crc:32,_Msg:Length/binary,_totalSize:32,Rest/binary>>) ->
	loop_find_txnlog_msgs(Zxid, Rest);
loop_find_txnlog_msgs(_Zxid, Rest) ->
	{not_found,Rest}.


load_from_file(Zxid,Fullname,L,Account) ->
	{ok,FD} = prim_file:open(Fullname,[read,binary]),
	{ok,Msgs,RemainAccount} = loop_load_txnlog_file0(Zxid,FD,L,Account,<<>>),
	prim_file:close(FD),
	{ok,Msgs,RemainAccount}.

loop_load_txnlog_file0(_,_FD,L,0,_) -> {ok,L,0};
loop_load_txnlog_file0(Zxid,FD,L,Account,B) ->
	case prim_file:read(FD, ?DEFAULT_LOAD_FILE_CHUNK_SIZE) of
		eof -> 
			{ok,L,Account};
		{ok,Data} ->
			{ok,NewMsgs,RestBin,RemainAccount} = loop_load_txnlog_msgs(Zxid,<<B/binary,Data/binary>>,L,Account),
			loop_load_txnlog_file0(Zxid,FD, NewMsgs, RemainAccount, RestBin)
	end.	
	
loop_load_txnlog_msgs(_,RestBin,L,0) -> {ok,L,RestBin,0};
loop_load_txnlog_msgs(Zxid,<<?TXN_LOG_FLAG,NZxid:64,Length:32,_Crc:32,Msg:Length/binary,_totalSize:32>>,L,all) when NZxid > Zxid->
	{ok,[{NZxid,Msg}|L],<<>>,all};
loop_load_txnlog_msgs(_Zxid,<<?TXN_LOG_FLAG,_:64,Length:32,_Crc:32,_Msg:Length/binary,_totalSize:32>>,L,all) ->
	{ok,L,<<>>,all};
loop_load_txnlog_msgs(Zxid,<<?TXN_LOG_FLAG,NZxid:64,Length:32,_Crc:32,Msg:Length/binary,_totalSize:32>>,L,Account) when NZxid > Zxid->
	{ok,[{NZxid,Msg}|L],<<>>,Account - 1};
loop_load_txnlog_msgs(_Zxid,<<?TXN_LOG_FLAG,_NZxid:64,Length:32,_Crc:32,_Msg:Length/binary,_totalSize:32>>,L,Account) ->
	{ok,L,<<>>,Account};
loop_load_txnlog_msgs(Zxid,<<?TXN_LOG_FLAG,NZxid:64,Length:32,_Crc:32,Msg:Length/binary,_totalSize:32,Rest/binary>>,L,all) when NZxid > Zxid->
	loop_load_txnlog_msgs(Zxid,Rest, [{NZxid,Msg}|L], all);
loop_load_txnlog_msgs(Zxid,<<?TXN_LOG_FLAG,_NZxid:64,Length:32,_Crc:32,_Msg:Length/binary,_totalSize:32,Rest/binary>>,L,all) ->
	loop_load_txnlog_msgs(Zxid,Rest, L, all);
loop_load_txnlog_msgs(Zxid,<<?TXN_LOG_FLAG,NZxid:64,Length:32,_Crc:32,Msg:Length/binary,_totalSize:32,Rest/binary>>,L,Account) when NZxid > Zxid->
	loop_load_txnlog_msgs(Zxid,Rest, [{NZxid,Msg}|L], Account - 1);
loop_load_txnlog_msgs(Zxid,<<?TXN_LOG_FLAG,_NZxid:64,Length:32,_Crc:32,_Msg:Length/binary,_totalSize:32,Rest/binary>>,L,Account) ->
	loop_load_txnlog_msgs(Zxid,Rest, L, Account);
loop_load_txnlog_msgs(_Zxid,RestBin, L, Account) ->
	{ok,L,RestBin,Account}.
	
	
	
	
	
	
	


