%%% -------------------------------------------------------------------
%%% Author  : clues
%%% Description :
%%%
%%% Created : Mar 29, 2013
%%% -------------------------------------------------------------------
-module(zab_sync_follower).

-behaviour(gen_fsm).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("zab.hrl").
-include("log.hrl").


%% --------------------------------------------------------------------
%% External exports
-export([start_link/1,
		 get_state/0,
		 stop/1,
		 commit/1,
		 broadcast/1,
		 ?ZAB_STATE_RECOVERING/2,
		 ?ZAB_STATE_SYNCING/2]).


%% gen_fsm callbacks
-export([init/1, state_name/2, state_name/3, handle_event/3,
	 handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {leader,
				leader_ref,
				last_zxid,
				last_msg,
				waited_msg_list = []   %% push msg to waited queue when follow is recovering status.
  }).

%% ====================================================================
%% External functions
%% ====================================================================
start_link(Leader) ->
	gen_fsm:start_link({local,?MODULE},?MODULE, [Leader], []).

-spec get_state() -> {ok,State::string()}.
get_state() ->
	gen_fsm:sync_send_all_state_event(?MODULE, get_state).

stop(Reason) ->
	gen_fsm:send_all_state_event(?MODULE, {stop,Reason}).

commit(_Zxid) ->
	gen_fsm:send_all_state_event(?MODULE, {commit,_Zxid}).

-spec broadcast(Propasal::#propose{}) ->{error,Reason::term()} | {Zxid::integer(),Node::node()}.
broadcast(Propasal) ->
%% 	?INFO_F("~p -- receive Propasal:~p~n",[?MODULE,Propasal]),
	gen_fsm:send_all_state_event(?MODULE, {broadcast,Propasal}).


%% ====================================================================
%% Server functions
%% ====================================================================
%% --------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, StateName, StateData}          |
%%          {ok, StateName, StateData, Timeout} |
%%          ignore                              |
%%          {stop, StopReason}
%% --------------------------------------------------------------------
init([Leader]) ->
	{ok,{LastZxid,LastMsg}} = file_txn_log:get_last_msg(),
	?INFO_F("~p -- init,leader:~p,last_zxid:~p~n",[?MODULE,Leader,LastZxid]),
	{ok,Pid} = rpc:call(Leader, zab_manager, get_sync_pid, []),
	Ref = erlang:monitor(process, Pid),
	gen_fsm:send_event(?MODULE, register),
    {ok, ?ZAB_STATE_RECOVERING, #state{leader=Leader,last_zxid=LastZxid,
										  leader_ref=Ref,last_msg=LastMsg}}.

%% --------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------
?ZAB_STATE_RECOVERING(register,#state{last_zxid=LastZxid,leader=Leader}=StateData) ->
	{ok,LastCommitZxid} = zab_apply_server:get_last_commit_zxid(),
	Req = #follower_info{id=node(),
						 last_zxid=LastZxid,
						 last_commit_zxid=LastCommitZxid},
	?DEBUG_F("~p -- start register info:~p to leader:~p",[?MODULE,Req,Leader]),
	ok = rpc:call(Leader, zab_sync_leader, register, [Req]),	
	gen_fsm:send_event(?MODULE, recover),
	{next_state, ?ZAB_STATE_SYNCING, StateData}.

?ZAB_STATE_SYNCING(recover,#state{last_zxid=LastZxid,leader=Leader,last_msg=LastMsg}=StateData) ->
	{ok,LastCommitZxid} = zab_apply_server:get_last_commit_zxid(),
	Req = #follower_info{id=node(),
						 last_zxid=LastZxid,
						 last_commit_zxid=LastCommitZxid},
	case rpc:call(Leader, zab_sync_leader, recover, [Req]) of
		{error,leader_unavailable} ->
			gen_fsm:send_event_after(500, recover),
			{next_state,?ZAB_STATE_SYNCING, StateData};	
		{error,Reason} ->
			?ERROR_F("~p -- recover from leader:~p error:~p~n",[?MODULE,Leader,Reason]),
			 {stop,recover_failed,StateData};
		{ok,#recover_response{type=?RECOVER_TYPE_TRUNC}} ->
			?INFO_F("~p -- truncate last zxid:~p when recovering.~n",[?MODULE,LastZxid]),
			ok = file_txn_log:truncate_last_zxid(),
			gen_fsm:send_event(?MODULE, recover),
			{ok,{NewLastZxid,TxnLogLastMsg}} = file_txn_log:get_last_msg(),
			{next_state,?ZAB_STATE_SYNCING, StateData#state{last_zxid=NewLastZxid,last_msg=TxnLogLastMsg}};
		{ok,#recover_response{type=?RECOVER_TYPE_DIFF,data= L,leader_last_zxid=LeaderLastZxid}} ->
			zab_apply_server:load_msg_to_commit(LastZxid),
			{ok,{NewLastZxid,NewLastMsg}} = sync_apply_logs(L,{LastZxid,LastMsg}),
			gen_fsm:send_event(?MODULE, {recover,LeaderLastZxid}),
			{next_state,?ZAB_STATE_SYNCING, StateData#state{last_zxid=NewLastZxid,last_msg=NewLastMsg}}
			
%% 			case  of
%% 				{ok,{LeaderLastZxid,NewLastMsg}} ->
%% 					?DEBUG_F("~p -- zxid diff,recover logs(~p) finished",[?MODULE,length(L)]),
%% 					rpc:cast(Leader, zab_sync_leader, ack, [#ack{type=?ACK_TYPE_SYNC,id = node()}]),
%% 					{ok,NewState} = loop_impl_waited_msgs(StateData#state{last_zxid=LeaderLastZxid,last_msg=NewLastMsg}),
%% 					{next_state,?ZAB_STATE_BROADCAST, NewState};
%% 				{ok,{NewLastZxid,NewLastMsg}} ->
%% 					?DEBUG_F("~p -- continue recover from ~p to ~p.~n",[?MODULE,NewLastZxid,LeaderLastZxid]),
%% 					gen_fsm:send_event(?MODULE, {recover,LeaderLastZxid}),
%% 					{next_state,?ZAB_STATE_SYNCING, StateData#state{last_zxid=NewLastZxid,last_msg=NewLastMsg}}
%% 			end
	end;
?ZAB_STATE_SYNCING({recover,RecoverLastZxid},#state{last_zxid=LastZxid,leader=Leader,last_msg=LastMsg}=StateData) when LastZxid >= RecoverLastZxid->
	?DEBUG_F("~p -- recover completed.~n",[?MODULE]),
	rpc:cast(Leader, zab_sync_leader, ack, [#ack{type=?ACK_TYPE_SYNC,id = node()}]),
	{ok,NewState} = loop_impl_waited_msgs(StateData#state{last_zxid=LastZxid,last_msg=LastMsg}),
	{next_state,?ZAB_STATE_BROADCAST, NewState};
?ZAB_STATE_SYNCING({recover,RecoverLastZxid},#state{last_zxid=LastZxid,leader=Leader,last_msg=LastMsg}=StateData) ->
	Req = #follower_info{id=node(),last_zxid=LastZxid,last_commit_zxid=LastZxid},
	case rpc:call(Leader, zab_sync_leader, recover, [Req]) of
		{ok,#recover_response{type=?RECOVER_TYPE_DIFF,data= L}} ->
			zab_apply_server:load_msg_to_commit(LastZxid),
			{ok,{NewLastZxid,NewLastMsg}} = sync_apply_logs(L,{LastZxid,LastMsg}),
			gen_fsm:send_event(?MODULE, {recover,RecoverLastZxid}),
			{next_state,?ZAB_STATE_SYNCING, StateData#state{last_zxid=NewLastZxid,last_msg=NewLastMsg}};
		_ ->
			?INFO_F("~p -- recover failed when recover diff.~n",[?MODULE]),
			{stop, normal, StateData}
	end.
	
	
state_name(Event, StateData) ->
    {next_state, state_name, StateData}.

%% --------------------------------------------------------------------
%% Func: StateName/3
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%% --------------------------------------------------------------------
state_name(Event, From, StateData) ->
    Reply = ok,
    {reply, Reply, state_name, StateData}.

%% --------------------------------------------------------------------
%% Func: handle_event/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------
handle_event({stop,Reason}, StateName, StateData) ->
	?ERROR_F("~p -- stop by reason: ~p~n",[?MODULE,Reason]),
    {stop, normal, StateData};


%% when commit zxid == lastZxid, commit msg
handle_event({commit,Zxid}, ?ZAB_STATE_BROADCAST, #state{last_zxid=Zxid,last_msg=Msg}=StateData) ->
	zab_apply_server:commit(Zxid,Msg),
    {next_state, ?ZAB_STATE_BROADCAST, StateData};	
%% otherwise, load_all msg(commitedZxid - Zxid) and commit them.
handle_event({commit,Zxid}, ?ZAB_STATE_BROADCAST, #state{last_zxid=FZxid}=StateData) when Zxid > FZxid->
	?DEBUG_F("~p -- load msg and commit from ~p to ~p~n",[?MODULE,FZxid,Zxid]),
	zab_apply_server:load_msg_to_commit(Zxid),
    {next_state, ?ZAB_STATE_BROADCAST, StateData};	
%% ignore if zxid < lastZxid.
handle_event({commit,Zxid}, ?ZAB_STATE_BROADCAST, StateData) ->
	?DEBUG_F("~p -- already commited zxid:~p~n",[?MODULE,Zxid]),
	{next_state, ?ZAB_STATE_BROADCAST, StateData};	
%% put to waited msg queue when recovering state.
handle_event({commit,_Zxid}=Msg, ?ZAB_STATE_SYNCING, #state{waited_msg_list=WaitedMsgs} = StateData) ->
	NewWaitedMsgs = lists:reverse([Msg|lists:reverse(WaitedMsgs)]),
	{next_state, ?ZAB_STATE_SYNCING, StateData#state{waited_msg_list=NewWaitedMsgs}};

handle_event({broadcast,#propose{}} = Msg, ?ZAB_STATE_SYNCING, #state{waited_msg_list=WaitedMsgs}=StateData) ->
	NewWaitedMsgs = lists:reverse([Msg|lists:reverse(WaitedMsgs)]),
	{next_state, ?ZAB_STATE_SYNCING, StateData#state{waited_msg_list=NewWaitedMsgs}};
handle_event({broadcast,#propose{zxid=Zxid,value=Val}}, ?ZAB_STATE_BROADCAST, #state{last_zxid=LastZxid,leader=Leader}=StateData) ->
	ok = check_zxid_uninterrupted(Zxid,LastZxid),
	ok = file_txn_log:append(Zxid, Val),
	Ack = #ack{type=?ACK_BROADCAST,id=node(),zxid=Zxid},
	rpc:cast(Leader, zab_sync_leader, ack, [Ack]),
	{next_state, ?ZAB_STATE_BROADCAST, StateData#state{last_zxid=Zxid,last_msg=Val}};	

handle_event(Event, StateName, StateData) ->
	?INFO_F("~p -- ignore event:~p on state:~p~n",[?MODULE,Event,StateName]),
    {next_state, StateName, StateData}.

%% --------------------------------------------------------------------
%% Func: handle_sync_event/4
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%% --------------------------------------------------------------------
handle_sync_event(get_state, From, StateName, StateData) ->
    {reply, {ok,StateName}, StateName, StateData};

handle_sync_event(Event, From, StateName, StateData) ->
    Reply = ok,
    {reply, Reply, StateName, StateData}.

%% --------------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------
handle_info({'DOWN', Ref, process, _Pid, Reason}, StateName, #state{leader=Leader,leader_ref=Ref}=StateData) ->
    ?INFO_F("~p -- leader:~p sync process down~n",[?MODULE,Leader]),
	{stop, {error,lost_leader}, StateData};

handle_info(Info, StateName, StateData) ->
    {next_state, StateName, StateData}.

%% --------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%% --------------------------------------------------------------------
terminate(Reason, StateName, StatData) ->
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/4
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState, NewStateData}
%% --------------------------------------------------------------------
code_change(OldVsn, StateName, StateData, Extra) ->
    {ok, StateName, StateData}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------
loop_impl_waited_msgs(#state{waited_msg_list=[]} = State) -> {ok,State};
loop_impl_waited_msgs(#state{waited_msg_list=[{broadcast,#propose{zxid=Zxid,value=Val}}|T]} = State) ->
	case file_txn_log:append(Zxid, Val) of
		ok -> ok;
		{error,ignored} -> ok
	end,
	loop_impl_waited_msgs(State#state{waited_msg_list=T,last_zxid=Zxid,last_msg=Val});
loop_impl_waited_msgs(#state{waited_msg_list=[{commit,Zxid}|T],last_zxid=Zxid,last_msg=Msg} = State) ->
	zab_apply_server:commit(Zxid,Msg),
	loop_impl_waited_msgs(State#state{waited_msg_list=T,last_zxid=Zxid,last_msg=Msg});
loop_impl_waited_msgs(#state{waited_msg_list=[{commit,Zxid}|T],last_zxid=FZxid,last_msg=Msg} = State) when Zxid > FZxid->
	?DEBUG_F("~p -- load msg and commit from ~p to ~p~n",[?MODULE,FZxid,Zxid]),
	zab_apply_server:load_msg_to_commit(Zxid),
	loop_impl_waited_msgs(State#state{waited_msg_list=T,last_zxid=Zxid,last_msg=Msg});
loop_impl_waited_msgs(#state{waited_msg_list=[{commit,Zxid}|T]} = State) ->
	?DEBUG_F("~p -- already commited zxid:~p~n",[?MODULE,Zxid]),
	loop_impl_waited_msgs(State#state{waited_msg_list=T}).


sync_apply_logs([],{Zxid,Data}) -> {ok,{Zxid,Data}};
sync_apply_logs([{Zxid,Data}|T],_) ->
	ok = file_txn_log:append(Zxid, Data),
	zab_apply_server:commit(Zxid,Data),
	sync_apply_logs(T,{Zxid,Data}).


check_zxid_uninterrupted(Zxid,CurLastZxid) ->
	{Repoch,Rcounter} = zab_util:split_zxid(Zxid),
	{Curepoch,Curcounter} = zab_util:split_zxid(CurLastZxid),
	if
		Curepoch == Repoch andalso Curcounter+1 == Rcounter ->
			ok;
		Curepoch < Repoch andalso Rcounter == 1 ->
			ok;
		true ->
			{error,zxid_interrupted}
	end.
		