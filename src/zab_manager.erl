%%% -------------------------------------------------------------------
%%% Author  : sunshine
%%% Description :
%%%
%%% Created : 2012-8-22
%%% -------------------------------------------------------------------
-module(zab_manager).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("zab.hrl").
-include("log.hrl").

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------
-export([start_link/0,
		 stop/0,
		 start_zab/0,
		 start_zab/2,
		 start_zab/3,
		 get_state/0,
		 get_sync_pid/0]).

%% --------------------------------------------------------------------
%% gen_server callbacks
%% --------------------------------------------------------------------
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {server_state = ?SERVER_STATE_LOOKING,
				election_mod = ?DEFAULT_ELECTION_MOD,
				followers = [],
				leader,
				server_pid,
				parent_pid,
				quorum,
				sync_pid
				}).

%% ====================================================================
%% External functions
%% ====================================================================
start_link() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

start_zab() ->
	gen_server:cast(?MODULE, start_zab).

start_zab(ZabNodes,Quorum) ->
	gen_server:cast(?MODULE, {start_zab,ZabNodes,Quorum,undefined}).

start_zab(ZabNodes,Quorum,Parent) ->
	gen_server:cast(?MODULE, {start_zab,ZabNodes,Quorum,Parent}).

get_state() ->
	gen_server:call(?MODULE, get_state,infinity).

get_sync_pid() ->
	gen_server:call(?MODULE, get_sync_pid,?DEFAULT_WAITING_TIMEOUT).

stop() ->
	gen_server:call(?MODULE, stop,infinity).

%% ====================================================================
%% Server functions
%% ====================================================================

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
	process_flag(trap_exit,true),
	start_zab(),
    {ok, #state{}}.

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
handle_call(get_state, _From, #state{server_state=SState,followers=Followers,leader=Leader} = State) ->
    {reply, {ok,{SState,Followers,Leader}}, State};

handle_call(get_sync_pid, _From, #state{sync_pid=Pid} = State) ->
    {reply, {ok,Pid}, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
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

handle_cast(start_zab, #state{server_state=?SERVER_STATE_LOOKING,server_pid=undefined} = State) ->
	?DEBUG_F("~p --~p start zab~n",[?MODULE,node()]),
	ElectionMod = zab_util:get_app_env(election_mode, [], ?DEFAULT_ELECTION_MOD),
	ZabNodes = zab_util:get_zab_nodes(),
	Quorum = length(ZabNodes) div 2 + 1,
%% 	Quorum = zab_util:get_app_env(quorum, [], DefaultQuorum),
	?DEBUG_F("~p --~p start zab of nodes:~p,quorum:~p~n",[?MODULE,node(),ZabNodes,Quorum]),
	{ok,Pid} = start_election(ElectionMod,ZabNodes, Quorum),
    {noreply, State#state{quorum=Quorum,election_mod=ElectionMod,server_pid=Pid}};

handle_cast({start_zab,ZabNodes,Quorum,ParentPid}, State) ->
	?DEBUG_F("~p --~p start zab of nodes:~p,quorum:~p~n",[?MODULE,node(),ZabNodes,Quorum]),
	ElectionMod = zab_util:get_app_env(election_mode, [], ?DEFAULT_ELECTION_MOD),
	{ok,Pid} = start_election(ElectionMod,ZabNodes, Quorum),
    {noreply, State#state{quorum=Quorum,parent_pid=ParentPid,election_mod=ElectionMod,server_pid=Pid}};

handle_cast(_Msg, State) ->
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
handle_info({election,_Vote} = Msg, #state{server_state=?SERVER_STATE_LOOKING,server_pid=undefined} = State) ->
	?DEBUG_F("~p --~p recive msg1:~p~n",[?MODULE,node(),_Vote]),
	start_zab(),
	self() ! Msg,
	{noreply, State};
handle_info({election,_Vote} = Msg, #state{server_state=?SERVER_STATE_LOOKING,election_mod=ElectionMod} = State) ->
	ElectionMod:handle_vote(Msg),
	{noreply, State};

handle_info({election,#notification{node=RemoteNode}} = Msg, #state{server_state=?SERVER_STATE_LEADING,followers=Followers} = State) ->
	case lists:member(RemoteNode,zab_util:get_zab_nodes()) of
		false ->
			{noreply, State};
		true ->
			election_msg_sender:notify_all_followers_to_commit([RemoteNode]),
			{noreply, State#state{followers=lists:sort([RemoteNode|lists:delete(RemoteNode, Followers)])}}
	end;

handle_info({election_ack,_Vote} = Msg, #state{server_state=?SERVER_STATE_LOOKING,server_pid=undefined} = State) ->
	start_zab(),
	self() ! Msg,
	{noreply, State};

handle_info({election_ack,_Vote} = Msg, #state{server_state=?SERVER_STATE_LOOKING,election_mod=ElectionMod} = State) ->
	ElectionMod:handle_vote_ingro_noexit(Msg),
	{noreply, State};

handle_info({leader,_} = Msg, #state{server_state=?SERVER_STATE_LOOKING,server_pid=undefined} = State) ->
	start_zab(),
	self() ! Msg,
	{noreply, State};

handle_info({leader,_} = Msg, #state{server_state=?SERVER_STATE_LOOKING,election_mod=ElectionMod} = State) ->
	ElectionMod:handle_vote_ingro_noexit(Msg),
	{noreply, State};

handle_info({leader_ack,_} = Msg, #state{server_state=?SERVER_STATE_LOOKING,server_pid=undefined} = State) ->
	start_zab(),
	self() ! Msg,
	{noreply, State};

handle_info({leader_ack,_} = Msg, #state{server_state=?SERVER_STATE_LOOKING,election_mod=ElectionMod} = State) ->
	ElectionMod:handle_vote_ingro_noexit(Msg),
	{noreply, State};


handle_info({leader_commit,_} = Msg, #state{server_state=?SERVER_STATE_LOOKING,server_pid=undefined} = State) ->
	start_zab(),
	self() ! Msg,
	{noreply, State};

handle_info({leader_commit,_} = Msg, #state{server_state=?SERVER_STATE_LOOKING,election_mod=ElectionMod} = State) ->
	ElectionMod:handle_vote_ingro_noexit(Msg),
	{noreply, State};

handle_info({election,leader,Followers} = Msg,#state{parent_pid=ParentPid,quorum=Quorum} = State) ->
	notify_parent(Msg,ParentPid),
	{ok,Pid} = zab_sync_leader:start_link(Quorum),
	{noreply, State#state{sync_pid=Pid,followers=Followers,leader=node(),server_state=?SERVER_STATE_LEADING}};

handle_info({election,follower,Leader} = Msg,#state{parent_pid=ParentPid} = State) ->
	notify_parent(Msg,ParentPid),
	{ok,Pid} = zab_sync_follower:start_link(Leader),
	{noreply, State#state{sync_pid=Pid,followers=[],leader=Leader,server_state=?SERVER_STATE_FOLLOWING}};


handle_info({'EXIT',Pid,Reason},#state{sync_pid=Pid} = State) ->
	?INFO("~p -- sync process exit with reason:~p~n",[?MODULE,Reason]),
	start_zab(),
	{noreply, State#state{followers=[],leader=undefined,server_state=?SERVER_STATE_LOOKING}};

handle_info({'EXIT',Pid,normal},#state{server_pid=Pid} = State) ->
	{noreply, State#state{server_pid=undefined}};

handle_info({'EXIT',Pid,Reason},#state{server_pid=Pid} = State) ->
	?ERROR_F("~p -- election process exit with reason:~p~n",[?MODULE,Reason]),
	start_zab(),
	{noreply, State#state{server_pid=undefined}};

handle_info({follower_down,Id},#state{followers=Followers} = State) ->
	{noreply, State#state{followers=lists:delete(Id, Followers)}};

handle_info(_Info, State) ->
	?INFO("~p -- msg:~p not implement.~n",[?MODULE,_Info]),
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
terminate(Reason, State) ->
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
start_election(ElectionMod,ZabNodes,Quorum) ->
	case ElectionMod:start_link() of
		{error,{already_started,Pid}} ->
			ok = ElectionMod:start_election(ZabNodes,Quorum),
			{ok,Pid};
		{ok,Pid} ->
			ok = ElectionMod:start_election(ZabNodes,Quorum),
			{ok,Pid}
	end.

notify_parent(_Msg,undefined) -> ok;
notify_parent(Msg,ParentPid) ->
	ParentPid ! Msg,
	ok.


wait_write() ->
	receive
		Msg ->
			ok
	after ?TIME_OUT_WRITE ->
			{error,time_out}
	end.
