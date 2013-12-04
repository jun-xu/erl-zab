%%% -------------------------------------------------------------------
%%% Author  : clues
%%% Description :
%%%
%%% Created : Mar 28, 2013
%%% -------------------------------------------------------------------
-module(zab_sync_leader).

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
		 ack/1,
		 sync/2,
		 register/1,
		 recover/1,
		 stop/1]).

%% gen_fsm callbacks
-export([init/1, state_name/2, state_name/3, handle_event/3,
	 handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {lastCommitZxid,
				quorum,
				last_zxid,
				followers=[],
				follower_nodes = [],
				ack_list=[],
				current_req = undefined,
				timer,
				current_epoch,
				waited_msgs= []}).

%% ====================================================================
%% External functions
%% ====================================================================
start_link(Quorum) ->
	gen_fsm:start_link({local,?MODULE},?MODULE, [Quorum], []).

get_state() ->
	gen_fsm:sync_send_all_state_event(?MODULE, get_state,infinity).

-spec register(Follower::#follower_info{}) -> ok.
register(Follower) ->
	gen_fsm:sync_send_all_state_event(?MODULE, {register,Follower},infinity).

-spec recover(Follower::#follower_info{}) -> {ok,Response::#recover_response{}}|{error,leader_unavailable}|{error,not_register}.
recover(Follower) ->
	gen_fsm:sync_send_all_state_event(?MODULE, {recover,Follower},infinity).

ack(Msg) ->
	gen_fsm:send_all_state_event(?MODULE, Msg).

sync(Msg,Caller) ->
	gen_fsm:send_all_state_event(?MODULE, {boradcast,Msg,Caller}).

stop(Reason) ->
	gen_fsm:send_all_state_event(?MODULE,{stop,Reason}).

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
init([1]) ->
	{ok,LastZxid} = file_txn_log:get_last_zxid(),
	{ok,LastCommitZxid} = zab_apply_server:get_last_commit_zxid(),
	{Epoch,_} = zab_util:split_zxid(LastZxid),
	?INFO_F("~p -- start quorum:1, lastZxid:~p, lastCommitZxid:~p new era:~p~n",[?MODULE,LastZxid,LastCommitZxid,Epoch+1]),
	case LastZxid > LastCommitZxid of
		true ->
			?INFO_F("~p -- commit all msg between ~p to ~p~n",[?MODULE,LastCommitZxid,LastZxid]),
			zab_apply_server:load_msg_to_commit(LastZxid),
			ok;
		false -> ok
	end,
	{ok, ?ZAB_STATE_BROADCAST, #state{current_epoch=Epoch+1,quorum=1,last_zxid=LastZxid,lastCommitZxid=LastCommitZxid,timer=undefined}};
init([Quorum]) ->
	{ok,LastZxid} = file_txn_log:get_last_zxid(),
	{ok,LastCommitZxid} = zab_apply_server:get_last_commit_zxid(),
	{Epoch,_} = zab_util:split_zxid(LastZxid),
	?INFO_F("~p -- start quorum:~p, lastZxid:~p, lastCommitZxid:~p new era:~p~n",[?MODULE,Quorum,LastZxid,LastCommitZxid,Epoch]),
	{State,Timer1} = case LastZxid of
						LastCommitZxid ->
							?INFO_F("~p -- lastZxid:~p equal to commitZxid:~p, change to ~p state.~n",[?MODULE,LastZxid,LastCommitZxid,syncing]),
							{?ZAB_STATE_SYNCING,undefined};
						_ ->
							%% when last zxid isnot commited,should be checked.
							?INFO_F("~p -- lastZxid:~p but commitZxid:~p, change to ~p state.~n",[?MODULE,LastZxid,LastCommitZxid,recovering]),
							Timer = gen_fsm:send_event_after(?DEFAULT_TIMEOUT_INIT, {error,leader_checked_timeout}),
							{?ZAB_STATE_RECOVERING,Timer}
					 end,
	{ok, State, #state{current_epoch=Epoch,quorum=Quorum,last_zxid=LastZxid,lastCommitZxid=LastCommitZxid,timer=Timer1}}.


?ZAB_STATE_BROADCAST({boradcast_timeout,Zxid},#state{current_req={Zxid,Caller,_}}=StateData) ->
	?ERROR_F("~p -- zxid sync faild,error:~p process will stop~n",[?MODULE,Zxid,timeout]),
	zab_util:notify_caller(Caller, {error,sync_timeout}),
	{stop, normal,StateData#state{current_req=undefined}}.

?ZAB_STATE_SYNCING({failed,timeout},StateData) ->
	?ERROR_F("~p -- leader recover timeout,process will stop~n",[?MODULE]),
	{stop,normal,StateData}.

?ZAB_STATE_RECOVERING({error,Reason},StateData) ->
	?ERROR_F("~p -- leader init timeout,process will stop~n",[?MODULE]),
	{stop,normal,StateData}.	


%% --------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------
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
	?INFO_F("~p -- not implement event ~p ~n",[?MODULE,Event]),
    {reply, ok, state_name, StateData}.

%% --------------------------------------------------------------------
%% Func: handle_event/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------
handle_event(#ack{type=?ACK_TYPE_SYNC,id=Id}=Ack, ?ZAB_STATE_SYNCING, 
			 #state{current_epoch=Epoch,ack_list=AckList,quorum=Q,followers=Followers,last_zxid=LastZxid,lastCommitZxid=LastCommitZxid}=StateData) ->
	?DEBUG_F("~p -- receive ack:~p~n",[?MODULE,Ack]),
	case lists:keyfind(Id, 2, Followers) of
		false ->	%% ignore request.
			{next_state, ?ZAB_STATE_SYNCING, StateData};
		_ ->
			case lists:member(Id, AckList) of
				true ->  %% already recoverid.
					{next_state, ?ZAB_STATE_SYNCING, StateData};
				false ->
					NewList = [Id|AckList],
					if
						length(NewList) +1 >= Q ->
							?INFO_F("~p -- start quorum:~p, lastZxid:~p, lastCommitZxid:~p new era:~p~n",[?MODULE,Q,LastZxid,LastCommitZxid,Epoch+1]),
							{next_state, ?ZAB_STATE_BROADCAST, StateData#state{current_epoch=Epoch+1,ack_list=[]}};
						true ->
							{next_state, ?ZAB_STATE_SYNCING, StateData#state{ack_list=NewList}}
					end
			end
	end;

handle_event(#ack{type=?ACK_BROADCAST,id=FNode,zxid=Zxid}, ?ZAB_STATE_BROADCAST, 
			 #state{follower_nodes=Nodes,ack_list=L,quorum=Q,current_req={Zxid,Caller,Msg},timer=Timer,waited_msgs=Waites}=StateData) ->
	case lists:member(FNode, L) of
		false ->
			NewList = [FNode|L],
			if length(NewList) +1 >= Q ->
				   	zab_util:notify_caller(Caller, {ok,Zxid}),
					zab_apply_server:commit(Zxid,Msg),
%% 					Nodes = lists:foldl(fun(#follower_info{id=Id},AccIn) ->[Id|AccIn] end, [], Followers),
					lists:foreach(fun(Node) ->
								  rpc:cast(Node, zab_sync_follower, commit, [Zxid]) end,Nodes),
					
					cancel_timer(Timer),
					case Waites of
						[] -> ok;
						_ -> self() ! broadcast_next_msg
					end,
					
					{next_state, ?ZAB_STATE_BROADCAST, StateData#state{ack_list=[],current_req=undefined,timer=undefined}};
				true ->
					{next_state, ?ZAB_STATE_BROADCAST, StateData#state{ack_list=NewList}}
			end;
		true ->
			{next_state, ?ZAB_STATE_BROADCAST, StateData}
	end;
	

handle_event({stop,Reason}, _StateName, StateData)  ->
	?ERROR_F("~p -- stop sync process with reason:~p~n",[?MODULE,Reason]),
	{stop,Reason, StateData};

%% bordcast msg.
%% if quorm == 1, self node is leader and follower, sync msg local.
handle_event({boradcast,Msg,Caller}, ?ZAB_STATE_BROADCAST,#state{quorum=1,current_epoch=Cepoch,last_zxid=LastZxid}=StateData) ->
	
	NewZxid = generate_new_zxid(Cepoch,LastZxid),
%% 	?DEBUG_F("~p -- sync msg:~p zxid:~p~n",[?MODULE,Msg,NewZxid]),
	ok = file_txn_log:append(NewZxid, Msg),
	zab_apply_server:commit(NewZxid,Msg),
	zab_util:notify_caller(Caller, {ok,NewZxid}),
	{next_state, ?ZAB_STATE_BROADCAST,StateData#state{last_zxid=NewZxid}};
%% other, notify all followers to sync msg. ex.include all not recovered followers.
handle_event({boradcast,Msg,Caller}, ?ZAB_STATE_BROADCAST, 
				  #state{current_epoch=Cepoch,follower_nodes=Nodes,current_req=undefined,last_zxid=LastZxid,timer=undefined}=StateData) ->
	NewZxid = generate_new_zxid(Cepoch,LastZxid),
	ok = file_txn_log:append(NewZxid, Msg),
%% 	Nodes = lists:foldl(fun(#follower_info{id=Id},AccIn) ->[Id|AccIn] end, [], L),
	lists:foreach(fun(Node) ->
						  rpc:cast(Node, zab_sync_follower, broadcast, [#propose{zxid=NewZxid,value=Msg}]) end, Nodes),
	Timer = gen_fsm:send_event_after(?DEFAULT_TIMEOUT_SYNC, {boradcast_timeout,NewZxid}),
	{next_state, ?ZAB_STATE_BROADCAST, StateData#state{last_zxid=NewZxid,timer=Timer,current_req={NewZxid,Caller,Msg}}};
%% push to waited list when pre msg is not been down.
handle_event({boradcast,Msg,Caller}, ?ZAB_STATE_BROADCAST, #state{waited_msgs=Waited}=StateData) ->
	{next_state, ?ZAB_STATE_BROADCAST, StateData#state{waited_msgs=[{Msg,Caller}|Waited]}};
handle_event({boradcast,Msg,Caller}, StateName, StateData) ->
	?INFO_F("~p -- sync msg:~p error:leader_unavailable in ~p.~n",[?MODULE,Msg,StateName]),
	zab_util:notify_caller(Caller, {error,leader_unavailable}),
    {next_state, StateName, StateData};

handle_event(Event, StateName, StateData) ->
	?DEBUG_F("~p -- not implement even:~p on state:~p~n",[?MODULE,Event,StateName]),
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
handle_sync_event(get_state, _From, StateName, StateData) ->
    {reply, {ok,StateName}, StateName, StateData};

handle_sync_event({register,#follower_info{id=Id}=Follower}, _From, ?ZAB_STATE_RECOVERING, 
				  #state{followers=L,follower_nodes=FL,quorum=Q,timer=Timer,last_zxid=LastZxid,lastCommitZxid=LastCommitZxid}=StateData) ->
	?INFO_F("~p -- register node info:~p~n",[?MODULE,Follower]),
	case lists:member(Id, FL) of
		false ->
			Ref = erlang:monitor(process, {zab_sync_follower,Id}),
			NewFollowers = [Follower#follower_info{ref=Ref}|L],
			NewFollowerNodes = [Id|FL],
			case loop_check_last_zxid(NewFollowers, Q, LastZxid, LastCommitZxid) of
				{incomplete,NewLastZxid} ->
					%% continued check last zxid when next follower registe.
					{reply, ok, ?ZAB_STATE_RECOVERING, StateData#state{last_zxid=NewLastZxid,followers=NewFollowers,follower_nodes=NewFollowerNodes}};
				{ok,NewLastCommitZxid} ->
					%% change to recovering state as last zxid successful checked.
					cancel_timer(Timer),
					{reply, ok, ?ZAB_STATE_SYNCING, StateData#state{followers=NewFollowers,follower_nodes=NewFollowerNodes,
																		timer=undefined,last_zxid=NewLastCommitZxid,lastCommitZxid=NewLastCommitZxid}}
			end;
		_ ->
			{reply, ok, ?ZAB_STATE_RECOVERING, StateData}		
	end;

handle_sync_event({register,#follower_info{id=Id}=Follower}, _From, StateName, 
				  #state{followers=L,follower_nodes=FL}=StateData) ->
	?INFO_F("~p -- register node info:~p~n",[?MODULE,Follower]),
	{NewFollowers,NewFL} = case lists:member(Id, FL) of
		false ->
			Ref = erlang:monitor(process, {zab_sync_follower,Id}),
			{[Follower#follower_info{ref=Ref}|L],[Id|FL]};
		_ ->
			{L,FL}
	end,
	{reply, ok, StateName, StateData#state{followers=NewFollowers,follower_nodes=NewFL}};

handle_sync_event({recover,_}, _From, ?ZAB_STATE_RECOVERING, StateData) ->
	{reply, {error,leader_unavailable}, ?ZAB_STATE_RECOVERING, StateData};
	
handle_sync_event({recover,#follower_info{id=Id}=Follower}, _From, StateName, 
				  #state{followers=L,last_zxid=LastZxid}=StateData) ->
	case lists:keyfind(Id, 2, L) of
		false ->
			?INFO_F("~p -- not register of follower:~p when recover.~n",[?MODULE,Follower]),
			{reply, {error,not_register}, StateName, StateData};
		_ ->
			?INFO_F("~p -- recover node info:~p~n",[?MODULE,Follower]),
			Response = recover_follower(Follower,LastZxid),
%% 			?DEBUG_F("~p -- response to follower:~p info:~p~n",[?MODULE,Id,Response]),
    		{reply, Response, StateName, StateData}
	end;

handle_sync_event(Event, From, StateName, StateData) ->
    Reply = ok,
    {reply, Reply, StateName, StateData}.

%% --------------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------


handle_info({'DOWN', Ref, process, _Pid, Reason}, StateName, #state{quorum=Quorum,followers=Followers,follower_nodes=FL}=StateData) ->
	case lists:keyfind(Ref, 5, Followers) of
		#follower_info{id=Id} ->
			?INFO_F("~p -- follower:~p sync process down with reason:~p~n",[?MODULE,Id,Reason]),
			NewFollowers = lists:keydelete(Id, 2, Followers),
			NewFL = lists:delete(Id, FL),
%% 			zab_util:notify_caller(zab_manager,{follower_down,Id}),
			if
				length(NewFollowers)+1 >= Quorum ->
					{next_state, StateName, StateData#state{followers=NewFollowers,follower_nodes=NewFL}};
				true ->
					self() ! {error,lost_quorum},
					{next_state, StateName, StateData#state{followers=NewFollowers,follower_nodes=NewFL}}
			end;
		false ->
			{next_state, StateName, StateData}
	end;

%% handle waited msg.
handle_info(broadcast_next_msg,?ZAB_STATE_BROADCAST,#state{current_epoch=Cepoch,follower_nodes=Nodes,waited_msgs=[{Msg,Caller}|T],
													 current_req=undefined,last_zxid=LastZxid,timer=undefined}=StateData) ->
	NewZxid = generate_new_zxid(Cepoch,LastZxid),
	ok = file_txn_log:append(NewZxid, Msg),
%% 	Nodes = lists:foldl(fun(#follower_info{id=Id},AccIn) ->[Id|AccIn] end, [], L),
	lists:foreach(fun(Node) ->
						  rpc:cast(Node, zab_sync_follower, broadcast, [#propose{zxid=NewZxid,value=Msg}]) end, Nodes),
	Timer = gen_fsm:send_event_after(?DEFAULT_TIMEOUT_SYNC, {boradcast_timeout,NewZxid}),
	{next_state, ?ZAB_STATE_BROADCAST, StateData#state{waited_msgs=T,last_zxid=NewZxid,timer=Timer,current_req={NewZxid,Caller,Msg}}};


handle_info({error,lost_quorum}, ?ZAB_STATE_BROADCAST, StateData) ->
	?ERROR_F("~p -- lost quorum.~n",[?MODULE]),	
	{stop, normal, StateData};
	
handle_info({error,Reason}, StateName, StateData) ->
	?ERROR_F("~p -- error,with reason:~p,process will stop~n",[?MODULE,Reason]),
    {stop, {error,Reason}, StateData};

handle_info(Info, StateName, StateData) ->
	?DEBUG_F("~p -- not implement info:~p~n",[?MODULE,Info]),
    {next_state, StateName, StateData}.

%% --------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%% --------------------------------------------------------------------
terminate(Reason, StateName,#state{waited_msgs=Msgs,current_req=Req} = StatData) ->
	?INFO_F("~p -- terminate by reason:~p in state:~p~n",[?MODULE,Reason,StateName]),
	notify_all_waited_msg(Msgs),
	case Req of
		undefined -> ok;
		{_,Caller,_} ->
			catch zab_util:notify_caller(Caller, {error,stop})
	end,
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

count_valid_last_zxid(Followers,Q,LastZxidLeader) ->
	if
		length(Followers)+1 >= Q ->
			lists:foldl(fun(#follower_info{last_zxid=LastZxid},{Match,Unmatch}) ->
								if LastZxid == LastZxidLeader ->
										{Match+1,Unmatch};
									true ->
										{Match,Unmatch+1}
								end
				end, {0,0}, Followers);
		true ->
			{-1,-1}
	end.

recover_follower(#follower_info{last_zxid=LastZxid},LeaderLastZxid) ->
	Req = #recover_response{leader_last_zxid=LeaderLastZxid},
	if
		LastZxid =< LeaderLastZxid ->
			case file_txn_log:load_logs(LastZxid,?RECOVER_LOAD_TXNLOG_PER_STEP) of
				{ok,L} ->
					{ok,Req#recover_response{type=?RECOVER_TYPE_DIFF,leader_last_zxid=LeaderLastZxid,data=L}};
				{error,not_found} ->
					{ok,Req#recover_response{type=?RECOVER_TYPE_TRUNC,leader_last_zxid=LeaderLastZxid}};
				Error ->
					Error
			end;
		LastZxid > LeaderLastZxid ->
			{ok,Req#recover_response{type=?RECOVER_TYPE_TRUNC,leader_last_zxid=LeaderLastZxid}}
	end.

-spec check_last_zxid(Followers::[#follower_info{}],Q::integer(),LastZxid::integer()) -> {ok,NewLastZxid::integer()} | incomplete.
check_last_zxid([#follower_info{last_commit_zxid=LastZxid}|_],_Q,LastZxid) -> {ok,LastZxid};
check_last_zxid(Followers,Q,LastZxid) when length(Followers) >= Q->
	{Match,UnMatch} = count_valid_last_zxid(Followers,Q,LastZxid),
	if
		Match+1 >= Q -> 
			%% when last zxid is valid, commit it.
			?INFO_F("~p -- check last zxid:~p ok.~n",[?MODULE,LastZxid]),
			zab_apply_server:load_msg_to_commit(LastZxid),
			{ok,LastZxid};
		UnMatch >= Q ->
			%% when last zxid is invalid, truncate it.
			?INFO_F("~p -- truncate last zxid:~p after checked.~n",[?MODULE,LastZxid]),
			ok = file_txn_log:truncate_last_zxid(),
			{ok,NewLastZxid} = file_txn_log:get_last_zxid(),
			{ok,NewLastZxid};				
		true ->
			incomplete
	end;
check_last_zxid(_,_,_) -> incomplete.

generate_new_zxid(CEpoch,LastZxid) ->
	case zab_util:split_zxid(LastZxid) of
		{CEpoch,_} ->
			LastZxid+1;
		{Epoch,_} when Epoch < CEpoch->
			<<NewZxid:64>> = <<CEpoch:16,1:48>>,
			NewZxid
	end.



cancel_timer(undefined) -> ok;
cancel_timer(Timer) -> gen_fsm:cancel_timer(Timer).


notify_all_waited_msg([]) -> ok;
notify_all_waited_msg([{_,Caller}|T]) ->
	catch zab_util:notify_caller(Caller, {error,stop}),
	notify_all_waited_msg(T).


%% check all zxid between LastCommitZxid to LastZxid. one by one increase.
-spec loop_check_last_zxid(Followers::[#follower_info{}],Q::integer(),LastZxid::integer(),LastCommitZxid::integer()) -> 
		  {ok,LastCommitZxid::integer()} | {incomplete,LastZxid::integer()}.
loop_check_last_zxid(_Followers,_Q,LastZxid,LastZxid) ->
	{ok,LastZxid};
loop_check_last_zxid(Followers,Q,LastZxid,LastCommitZxid) ->
	?DEBUG_F("~p -- loop check last zxid:~p of lastcommitzxid:~p~n",[?MODULE,LastZxid,LastCommitZxid]),
	case check_last_zxid(Followers,Q,LastZxid) of
		incomplete ->
			%% continued check last zxid when next follower registe.
			{incomplete,LastZxid};
		{ok,LastZxid} ->
			%% change to recovering state as last zxid successful checked.
			{ok,LastZxid};
		{ok,NewLastZxid} ->
			loop_check_last_zxid(Followers, Q, NewLastZxid, LastCommitZxid)
	end.








