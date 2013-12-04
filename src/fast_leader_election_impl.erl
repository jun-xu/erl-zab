%%% -------------------------------------------------------------------
%%% Author  : sunshine
%%% Description :
%%%
%%% Created : 2012-9-4
%%% -------------------------------------------------------------------
-module(fast_leader_election_impl).

-behaviour(gen_fsm).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("log.hrl").
-include("zab.hrl").

-ifdef('TEST').
-compile(export_all).
-endif.

%% --------------------------------------------------------------------
%% External exports
-export([start_link/0,start_election/2,handle_vote/1,get_state/0,stop/0,handle_vote_ingro_noexit/1,
		 ?ELECTION_STATE_NAME_LOOKING/2,?ELECTION_STATE_NAME_FOLLOWING/2,?ELECTION_STATE_NAME_LEADERING/2]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
	 handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

%% ====================================================================
%% External functions
%% ====================================================================
start_link() ->
	gen_fsm:start_link({local,?MODULE},?MODULE, [], []).

start_election(ZabNodes,Quorum) ->
	gen_fsm:send_all_state_event(?MODULE,{start_election,self(),ZabNodes,Quorum}).


handle_vote(Msg) ->
  	gen_fsm:send_event(?MODULE, Msg).

handle_vote_ingro_noexit(Msg) ->
  	case catch gen_fsm:send_event(?MODULE, Msg) of
		{'EXIT',Reason} ->
			?ERROR_F("~p -- send event failed with reason:~p",[?MODULE,Reason]);
		_ ->
			ignored
	end.

get_state() ->
	gen_fsm:sync_send_all_state_event(?MODULE, get_state,infinity).

stop() ->
	gen_fsm:sync_send_all_state_event(?MODULE, stop,infinity).

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
init([]) ->
	SelfNode = node(),
	{ok,Zxid} = file_txn_log:get_last_zxid(),
	{Epoch,_} = zab_util:split_zxid(Zxid),
	?INFO_F("~p -- start election with args:~p~n",[?MODULE,{SelfNode,Epoch,Zxid}]),
    {ok, ?ELECTION_STATE_NAME_LOOKING,#election{epoch=Epoch,zxid=Zxid,node=SelfNode,
				   proposed_zxid=Zxid,proposed_epoch=Epoch,proposed_leader=SelfNode}}.
	
%% --------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% msg:
%% 			{election,notification}
%% 			{election_ack,notification}
%% 			{statistics_votes,ECount}
%% 			{election_failed,ECount,Reason}
%% 			{election_timeout,ECount}
%% 			{publish_leader,ECount}
%% 			{leader,{Leader,LeaderEpoch,LeaderZxid,LeaderECount}
%% 			{election_failed,ECount,leader_timeout}
%% 			{start_commit_leader,ECount}
%% 			{leader_ack,{Follower,LeaderECount,ok|failed}
%% 			{leader_commit,Leader}
%% --------------------------------------------------------------------
%% ------------------------------------looking state ----------------------------------------
?ELECTION_STATE_NAME_LOOKING({election_ack,#notification{node=NNode,proposed_leader=NLeader,proposed_epoch=NEpoch,ecount=ECount,proposed_zxid=NZxid}= Vote} = ACK,
							 #election{timer=Timer,zab_nodes=Zabs,votes=Votes,ecount=ECount,proposed_zxid=CurZxid,proposed_epoch=CurEpoch,proposed_leader=LeaderNode,quorum=Quorum} = State) ->
	?DEBUG_F("~p(looking) -- receive node:~p vote ack:~p,~p~n",[?MODULE,NNode,{NNode,NEpoch,NZxid},{node(),CurEpoch,CurZxid}]),
	case total_order_predicate(NLeader, NEpoch, NZxid, LeaderNode, CurEpoch, CurZxid) of
		new_leader ->
			change_to_following_state_when_rev_ack(Timer, State, NEpoch, NLeader, NZxid,ECount);
		self ->
			Votes0 =[{NNode,Vote}| proplists:delete(NNode, Votes)],
			?DEBUG_F("~p -- predicate_votes:~p~n zabs:~p~n",[?MODULE,Votes0,Zabs]),
			predicate_votes(Votes0,Quorum,Timer,ECount),
			{next_state,?ELECTION_STATE_NAME_LOOKING, State#election{votes=Votes0}}
	end;
?ELECTION_STATE_NAME_LOOKING({election,#notification{node=NNode,proposed_leader=NLeader,proposed_epoch=NEpoch,ecount=VoteECount,proposed_zxid=NZxid}} = Vote,
							  #election{timer=Timer,epoch=Epoch,node=Node,zxid=Zxid,zab_nodes=AllZabNodes,ecount=ECount,
					  proposed_zxid=CurZxid,proposed_epoch=CurEpoch,proposed_leader=LeaderNode} = State) ->
	?DEBUG_F("~p -- receive node:~p vote:~p~n",[?MODULE,NNode,Vote]),
	case total_order_predicate(NLeader, NEpoch, NZxid, LeaderNode, CurEpoch, CurZxid) of
		new_leader ->
			%% change to following...
			change_to_following_state(Timer, NNode, Epoch, VoteECount, Node, NEpoch, NLeader, NZxid, Zxid, State,ECount,CurEpoch,CurZxid,LeaderNode);
		self ->
			send_election_ack(NNode,#notification{epoch=Epoch,ecount=VoteECount,node=Node,proposed_epoch=CurEpoch,proposed_leader=LeaderNode,proposed_zxid=CurZxid,zxid=Zxid}),
			case zab_util:contains(NNode, AllZabNodes) of
				true ->
					{next_state, ?ELECTION_STATE_NAME_LOOKING,State};
				%% sent nodes does not contain the new node.
				false ->
					?INFO_F("~p -- ~p discover new node:~p~n",[?MODULE,node(),NNode]),
					%%1.send notification to the node
					send_notification(State,[NNode],-1),
					%%2.add node to zabnodes.
					{next_state, ?ELECTION_STATE_NAME_LOOKING,State#election{zab_nodes=[NNode|AllZabNodes]}}
			end
	end;

?ELECTION_STATE_NAME_LOOKING({leader,{Leader,LeaderEpoch,LeaderZxid,LeaderECount}=Notify},
							 #election{timer=Timer,ecount=ECount,proposed_zxid=CurZxid,proposed_epoch=CurEpoch,proposed_leader=LeaderNode} = State) ->
	?DEBUG_F("~p -- ~p receive leader notify:~p on state:~p~n",[?MODULE,node(),Notify,?ELECTION_STATE_NAME_LOOKING]),
	case total_order_publish_leader(Leader, LeaderEpoch, LeaderZxid, LeaderNode, CurEpoch, CurZxid) of
		new_leader ->
			send_leader_ack(Leader, LeaderECount, ok),
			cancel_timer(Timer),
			FollowerTimerOutTimer = gen_fsm:send_event_after(?DEFAULT_WAITING_TIMEOUT, {election_failed,ECount,following_timeout}),
			{next_state,?ELECTION_STATE_NAME_FOLLOWING, State#election{votes=[],proposed_epoch=LeaderEpoch,proposed_leader=Leader,timer=FollowerTimerOutTimer,
									 proposed_zxid=LeaderZxid,followers=[],followers_acks=[]}};
		self ->
			send_leader_ack(Leader, LeaderECount, failed),
			{next_state, ?ELECTION_STATE_NAME_LOOKING,State}
	end;

?ELECTION_STATE_NAME_LOOKING({statistics_votes,ECount},#election{timer=Timer,ecount=ECount,quorum=Quorum,votes=Votes} = State) ->
	?DEBUG_F("~p -- ~p statistics votes:~p,quorum:~p~n",[?MODULE,node(),Votes,Quorum]),
	cancel_timer(Timer),
	if length(Votes)+1 >= Quorum ->
			%% self is leader.
			gen_fsm:send_event(?MODULE,{publish_leader,ECount}),
			?INFO_F("~p -- self:~p [won] the election.~n",[?MODULE,node()]),
			{next_state, ?ELECTION_STATE_NAME_LEADERING,State#election{timer=undefined}};
		true ->
			?INFO_F("~p -- self:~p [lose]  by result:~p~n",[?MODULE,node(),votes_not_enough]),
			gen_fsm:send_event(?MODULE,{election_failed,ECount,votes_not_enough}),
			{next_state,?ELECTION_STATE_NAME_LOOKING,State}
	end;
?ELECTION_STATE_NAME_LOOKING({election_failed,ECount,Reason},#election{ecount=ECount,timer=Timer} = State) ->
	?INFO_F("~p-- ~p election failed by reason:~p~n",[?MODULE,node(),Reason]),
	cancel_timer(Timer),
	NewTimer = gen_fsm:send_event_after(?DEFAULT_INTERVAL_TIMEROUT, {election_timeout,ECount}),
	{next_state,?ELECTION_STATE_NAME_LOOKING,State#election{timer=NewTimer}};

?ELECTION_STATE_NAME_LOOKING({election_timeout,ECount},#election{zab_nodes=Zabs,ecount=ECount1,quorum=Quorum} = State) ->
	?DEBUG_F("~p -- ~p election timeout on state:~p~n",[?MODULE,node(),?ELECTION_STATE_NAME_LOOKING]),
	NewState = clear_state(State),
	{ok,Timer,NewEcount,Vote} = start_election0(NewState),
	Votes0 =[],
	?DEBUG_F("~p -- predicate_votes:~p~n zabs:~p~n",[?MODULE,Votes0,Zabs]),
	predicate_votes(Votes0,Quorum,Timer,NewEcount),
	{next_state, ?ELECTION_STATE_NAME_LOOKING,NewState#election{timer=Timer,ecount=NewEcount,votes=Votes0}};
 
?ELECTION_STATE_NAME_LOOKING({leader_commit,Leader},#election{from=From,timer=Timer}=State) ->
	cancel_timer(Timer),
	%% ask manager to start follower mod.
	zab_util:notify_caller(zab_manager, {election,follower,Leader}),
	{stop, normal, State};

?ELECTION_STATE_NAME_LOOKING(Event,State) ->
	?INFO_F("~p -- not implement event:~p on state:~p~n",[?MODULE,Event,?ELECTION_STATE_NAME_LOOKING]),
	{next_state, ?ELECTION_STATE_NAME_LOOKING,State}.

%% ------------------------------------following state ---------------------------------------- 
?ELECTION_STATE_NAME_FOLLOWING({election,#notification{node=NNode,proposed_leader=NLeader,proposed_epoch=NEpoch,ecount=VoteECount,proposed_zxid=NZxid}} = Vote,
			 #election{timer=Timer,epoch=Epoch,node=Node,zxid=Zxid,ecount=ECount,
					  proposed_zxid=CurZxid,proposed_epoch=CurEpoch,proposed_leader=LeaderNode} = State) ->
	?DEBUG_F("~p -- receive node:~p vote:~p~n",[?MODULE,NNode,Vote]),
	case total_order_predicate(NLeader, NEpoch, NZxid, LeaderNode, CurEpoch, CurZxid) of
		new_leader ->
			%% change to following...
			change_to_following_state(Timer, NNode, Epoch, VoteECount, Node, NEpoch, NLeader, NZxid, Zxid, State,ECount,CurEpoch,CurZxid,LeaderNode);
		self ->
			send_election_ack(NNode,#notification{epoch=Epoch,ecount=VoteECount,node=Node,proposed_epoch=CurEpoch,proposed_leader=LeaderNode,proposed_zxid=CurZxid,zxid=Zxid}),
			{next_state, ?ELECTION_STATE_NAME_FOLLOWING,State#election{votes=[]}}
	end;

?ELECTION_STATE_NAME_FOLLOWING({election_ack,#notification{node=NNode,proposed_leader=NLeader,proposed_epoch=NEpoch,ecount=ECount,proposed_zxid=NZxid}} = ACK,
			  #election{timer=Timer,ecount=ECount,
					  proposed_zxid=CurZxid,proposed_epoch=CurEpoch,proposed_leader=LeaderNode} = State) ->
		?DEBUG_F("~p(follow) --receive node:~p vote ack:~p~n",[?MODULE,NNode,ACK]),
	case total_order_predicate(NLeader, NEpoch, NZxid, LeaderNode, CurEpoch, CurZxid) of
		new_leader ->
			%% change to following...
			change_to_following_state_when_rev_ack(Timer, State, NEpoch, NLeader, NZxid,ECount);
		self ->
			{next_state, ?ELECTION_STATE_NAME_FOLLOWING,State#election{votes=[]}}
	end;

?ELECTION_STATE_NAME_FOLLOWING({leader,{Leader,LeaderEpoch,LeaderZxid,LeaderECount}=Notify},
							 #election{timer=Timer,ecount=ECount,proposed_zxid=LeaderZxid,proposed_epoch=LeaderEpoch,proposed_leader=Leader} = State) ->
	?DEBUG_F("~p -- ~p receive leader notify:~p on state:~p~n",[?MODULE,node(),Notify,?ELECTION_STATE_NAME_FOLLOWING]),
	send_leader_ack(Leader, LeaderECount, ok),
	cancel_timer(Timer),
	FollowerTimerOutTimer = gen_fsm:send_event_after(?DEFAULT_WAITING_TIMEOUT, {election_failed,ECount,following_timeout}),
	{next_state,?ELECTION_STATE_NAME_FOLLOWING, State#election{timer=FollowerTimerOutTimer}};


?ELECTION_STATE_NAME_FOLLOWING({election_failed,ECount,Reason},#election{ecount=ECount,timer=Timer}=State) ->
	?INFO_F("~p -- ~p election failed by reason:~p~n",[?MODULE,node(),Reason]),
	cancel_timer(Timer),
	NewTimer = gen_fsm:send_event_after(?DEFAULT_INTERVAL_TIMEROUT, {election_timeout,ECount}),
	{next_state,?ELECTION_STATE_NAME_LOOKING,State#election{timer=NewTimer}};

?ELECTION_STATE_NAME_FOLLOWING({leader_commit,Leader},#election{proposed_leader=Leader,timer=Timer}=State) ->
	cancel_timer(Timer),
	%% ask manager to start follower mod.
	zab_util:notify_caller(zab_manager, {election,follower,Leader}),
	{stop, normal, State};

?ELECTION_STATE_NAME_FOLLOWING(Event,State) ->
	?INFO_F("~p -- not implement event:~p on state:~p~n",[?MODULE,Event,?ELECTION_STATE_NAME_FOLLOWING]),
	{next_state, ?ELECTION_STATE_NAME_FOLLOWING,State}.

%%-------------------------------------- leading state-----------------------------------------------
?ELECTION_STATE_NAME_LEADERING({election,#notification{node=NNode,proposed_leader=NLeader,proposed_epoch=NEpoch,ecount=VoteECount,proposed_zxid=NZxid}} = Vote,
			  #election{timer=Timer,epoch=Epoch,node=Node,zxid=Zxid,ecount=ECount,
					  proposed_zxid=CurZxid,proposed_epoch=CurEpoch,proposed_leader=LeaderNode} = State) ->																											
	?DEBUG_F("~p --receive node:~p vote:~p~n",[?MODULE,NNode,Vote]),
	case total_order_predicate(NLeader, NEpoch, NZxid, LeaderNode, CurEpoch, CurZxid) of
		new_leader ->
			%% change to following...
			change_to_following_state(Timer, NNode, Epoch, VoteECount, Node, NEpoch, NLeader, NZxid, Zxid, State,ECount,CurEpoch,CurZxid,LeaderNode);
		self ->
			%%ignore.
			{next_state, ?ELECTION_STATE_NAME_LEADERING,State}
	end;
?ELECTION_STATE_NAME_LEADERING({election_ack,#notification{node=NNode,proposed_leader=NLeader,proposed_epoch=NEpoch,ecount=ECount,proposed_zxid=NZxid}} = ACK,
			  #election{timer=Timer,ecount=ECount,
					  proposed_zxid=CurZxid,proposed_epoch=CurEpoch,proposed_leader=LeaderNode} = State) ->
	?DEBUG_F("~p(leading) --receive node:~p vote ack:~p~n",[?MODULE,NNode,ACK]),
	case total_order_predicate(NLeader, NEpoch, NZxid, LeaderNode, CurEpoch, CurZxid) of
		new_leader ->
			%% change to following...
			change_to_following_state_when_rev_ack(Timer, State, NEpoch, NLeader, NZxid,ECount);
		self ->
			%%ignore.
			{next_state, ?ELECTION_STATE_NAME_LEADERING,State}
	end;

?ELECTION_STATE_NAME_LEADERING({election_failed,ECount,Reason},#election{ecount=ECount,timer=Timer}=State) ->
	?INFO_F("~p -- ~p election failed by reason:~p on state:~p~n",[?MODULE,node(),Reason,?ELECTION_STATE_NAME_LEADERING]),
	cancel_timer(Timer),
	NewTimer = gen_fsm:send_event_after(?DEFAULT_INTERVAL_TIMEROUT, {election_timeout,ECount}),
	{next_state,?ELECTION_STATE_NAME_LOOKING,State#election{timer=NewTimer}};

%% Follower approve of self to become new leader.
?ELECTION_STATE_NAME_LEADERING({leader_ack,{Follower,LeaderECount,ok}=ACK},#election{quorum=Quorum,timer=Timer,followers=Followers,followers_acks=ACKS,ecount=LeaderECount} = State) ->
	?DEBUG_F("~p --~p [approve] of ~p to become Leader in ~p rounds.~n",[?MODULE,Follower,node(),LeaderECount]),
	case lists:member(Follower, Followers) of
		true ->
			NewACKS =[ACK | proplists:delete(Follower, ACKS)],
			predicate_leader_acks(Quorum,NewACKS,Timer,LeaderECount),
			{next_state, ?ELECTION_STATE_NAME_LEADERING,State#election{followers_acks=NewACKS}};
		false ->
%% 			ignore
			{next_state, ?ELECTION_STATE_NAME_LEADERING,State}	
	end;
%% Follower oppose of self to become new leader.
?ELECTION_STATE_NAME_LEADERING({leader_ack,{Follower,LeaderECount,_}},#election{quorum=Quorum,followers=Followers,timer=Timer,followers_acks=ACKS,ecount=LeaderECount} = State) ->
	?DEBUG_F("~p --~p [against] ~p to become Leader in ~p rounds.~n",[?MODULE,Follower,node(),LeaderECount]),
	case lists:member(Follower, Followers) of
		true ->
			NewFollowers = lists:delete(Follower, Followers),
			predicate_leader_acks(Quorum,ACKS,Timer,LeaderECount),
			{next_state, ?ELECTION_STATE_NAME_LEADERING,State#election{followers=NewFollowers}};
		false ->
%% 			ignore
			{next_state, ?ELECTION_STATE_NAME_LEADERING,State}	
	end;

?ELECTION_STATE_NAME_LEADERING({start_commit_leader,LeaderECount},#election{zab_nodes=ZabNodes,followers=Followers,from=From,quorum=Quorum,timer=Timer,followers_acks=ACKS,ecount=LeaderECount} = State) ->
	?DEBUG_F("~p -- ~p commit leader ack:~p,quorum:~p~n",[?MODULE,node(),ACKS,Quorum]),
	cancel_timer(Timer),
	if length(ACKS) >= (Quorum - 1) ->
			%% self is leader.
			?DEBUG_F("~p -- leader:~p [commit] to followers:~p.~n",[?MODULE,node(),Followers]),
			ACKFollowers = filter_ack_followers(ACKS),
			notify_all_followers_to_commit(lists:delete(node(), ZabNodes)),
			%% ask manager to start leader mod.
			?DEBUG_F("~p -- ~p ask manager change to leader,~n",[?MODULE,node()]),
			zab_manager ! {election,leader,lists:sort(Followers)},
			{stop,normal,State#election{followers=ACKFollowers,timer=undefined}};
		true ->
			?INFO_F("~p -- leader:~p [lose]  by result:~p~n",[?MODULE,node(),leader_acks_not_enough]),
			gen_fsm:send_event(?MODULE,{leader_failed,LeaderECount,leader_acks_not_enough}),
			{next_state,?ELECTION_STATE_NAME_LEADERING,State#election{timer=undefined}}
	end;

?ELECTION_STATE_NAME_LEADERING({leader,{Leader,LeaderEpoch,LeaderZxid,LeaderECount}=Notify},
							 #election{timer=Timer,ecount=ECount,proposed_zxid=CurZxid,proposed_epoch=CurEpoch,proposed_leader=LeaderNode} = State) ->
	?DEBUG_F("~p -- ~p receive leader notify:~p on state:~p~n",[?MODULE,node(),Notify,?ELECTION_STATE_NAME_LEADERING]),
	case total_order_publish_leader(Leader, LeaderEpoch, LeaderZxid,LeaderNode, CurEpoch, CurZxid) of
		new_leader ->
			send_leader_ack(Leader, LeaderECount, ok),
			cancel_timer(Timer),
			FollowerTimerOutTimer = gen_fsm:send_event_after(?DEFAULT_WAITING_TIMEOUT, {election_failed,ECount,following_timeout}),
			{next_state,?ELECTION_STATE_NAME_FOLLOWING, State#election{votes=[],proposed_epoch=LeaderEpoch,proposed_leader=Leader,timer=FollowerTimerOutTimer,
									 proposed_zxid=LeaderZxid,followers=[],followers_acks=[]}};
		self ->
			send_leader_ack(Leader, LeaderECount, failed),
			{next_state, ?ELECTION_STATE_NAME_LEADERING,State}
	end;

?ELECTION_STATE_NAME_LEADERING({publish_leader,ECount},#election{zab_nodes=ZabNodes,timer=Timer,ecount=ECount,votes=Votes} = State) ->
	cancel_timer(Timer),
	Followers = filter_all_followers_except_self(Votes),
	NewECount = ECount,
	NewState = State#election{followers=Followers,ecount=NewECount,followers_acks=[]},
	send_leader_notification(NewState,lists:delete(node(), ZabNodes)),
	LeaderTimerOutTimer = gen_fsm:send_event_after(?DEFAULT_WAITING_TIMEOUT, {start_commit_leader,ECount}),
	{next_state, ?ELECTION_STATE_NAME_LEADERING,NewState#election{timer=LeaderTimerOutTimer}};

?ELECTION_STATE_NAME_LEADERING(Event,State) ->
	?INFO_F("~p -- not implement event:~p on ~p~n",[?MODULE,Event,?ELECTION_STATE_NAME_LEADERING]),
	{next_state, ?ELECTION_STATE_NAME_LEADERING,State}.

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
%%-------------------------------------- all state msg-----------------------------------------------
handle_event({start_election,From,_,1}, ?ELECTION_STATE_NAME_LOOKING, State) ->
	?INFO_F("~p -- local leader:~p... ~n",[?MODULE,node()]),
	zab_util:notify_caller(zab_manager, {election,leader,[]}),
	{stop,normal,State};
    
handle_event({start_election,From,ZabNodes,Quorum}, ?ELECTION_STATE_NAME_LOOKING, #election{epoch=Epoch,node=SelfNode,
				  			proposed_epoch=Epoch,proposed_leader=SelfNode} = State) ->
	?INFO_F("~p -- start election on ~p... ~n",[?MODULE,node()]),
	{ok,Timer,NewEcount,Vote} = start_election0(State#election{zab_nodes=ZabNodes,quorum=Quorum}),
	Votes0 =[],
%% 	?DEBUG_F("~p -- predicate_votes:~p~n zabs:~p~n",[?MODULE,Votes0,ZabNodes]),
%% 	predicate_votes(Votes0,Quorum,Timer,NewEcount),
    {next_state,?ELECTION_STATE_NAME_LOOKING, State#election{zab_nodes=ZabNodes,quorum=Quorum,
															 timer=Timer,ecount=NewEcount,from=From,votes=Votes0}};

handle_event(Event, StateName, StateData) ->
	?INFO_F("~p -- not implement msg:~p on state:~p~n",[?MODULE,Event,StateName]),
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


handle_sync_event(stop, _From, _StateName, StateData) ->
    {stop, normal, ok, StateData};

handle_sync_event(Event, From, StateName, StateData) ->
    {reply, ok, StateName, StateData}.


%% --------------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------
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
start_election0(#election{timer=OldTimer,quorum=Quorum,timeout=Timeout,zab_nodes=ZabNodes,ecount=ECount} = State) ->
	?DEBUG_F("~p -- ~p start election with nodes:~p,quorum:~p~n",[?MODULE,node(),ZabNodes,Quorum]),
	cancel_timer(OldTimer),
	NewEcount = ECount+1,
	{ok,Vote} = send_notification(State#election{ecount=NewEcount},ZabNodes,Quorum),
	Timer = gen_fsm:send_event_after(Timeout, {statistics_votes,NewEcount}),
	{ok,Timer,NewEcount,Vote}.


clear_state(#election{timer=Timer,zab_nodes=ZabNodes,quorum=Quorum,ecount=Ecount}) ->
	cancel_timer(Timer),
	{ok,Zxid} = file_txn_log:get_last_zxid(),
	{Epoch,_} = zab_util:split_zxid(Zxid),
	SelfNode = node(),
	#election{epoch=Epoch,zxid=Zxid,node=SelfNode,zab_nodes=ZabNodes,quorum=Quorum,proposed_zxid=Zxid,
			  proposed_epoch=Epoch,proposed_leader=SelfNode,votes=[],timer=undefined,followers=[],followers_acks=[],ecount=Ecount+1}.

send_leader_notification(#election{ecount=ECount,node=Leader,epoch=LeaderEpoch,zxid=LeaderZxid},Followers) ->
	Notify = {Leader,LeaderEpoch,LeaderZxid,ECount},
	election_msg_sender:public_leader_to_nodes(Followers, Notify),
	ok.

send_leader_ack(Leader,LeaderECount,Msg) ->
	election_msg_sender:send_ack_to_leader(Leader, {node(),LeaderECount,Msg}),
	ok.

send_notification(Election,ZabNodes,_Quorum) ->
	Vote = change_to_vote(Election),
	?DEBUG_F("~p -- ~p send vote:~p to nodes:~p~n",[?MODULE,node(),Vote,ZabNodes]),
	election_msg_sender:send_vote_to_nodes(ZabNodes, Vote),
	{ok,Vote}.

change_to_vote(#election{epoch=Epoch,zxid=Zxid,node=SelfNode,ecount=ECount,
				   proposed_zxid=PZxid,proposed_epoch=PEpoch,proposed_leader=PNode}) ->
	#notification{epoch=Epoch,node=SelfNode,proposed_epoch=PEpoch,proposed_leader=PNode,
				  proposed_zxid=PZxid,zxid=Zxid,ecount=ECount}.

total_order_predicate(_NNode,NEpoch,_NZxid,_CurNode,CurEpoch,_CurZxid) when NEpoch > CurEpoch ->
	new_leader;
total_order_predicate(_NNode,NEpoch,_NZxid,_CurNode,CurEpoch,_CurZxid) when NEpoch < CurEpoch ->
	self;
total_order_predicate(_NNode,Epoch,NZxid,_CurNode,Epoch,CurZxid) when NZxid > CurZxid ->
	new_leader;
total_order_predicate(_NNode,Epoch,NZxid,_CurNode,Epoch,CurZxid) when NZxid < CurZxid ->
	self;
total_order_predicate(NNode,Epoch,Zxid,CurNode,Epoch,Zxid) when NNode > CurNode ->
	new_leader;
total_order_predicate(_,_,_,_,_,_) -> self.

total_order_publish_leader(NNode,NEpoch,NZxid,CurNode,CurEpoch,CurZxid) ->
	if
		NEpoch > CurEpoch; NZxid > CurZxid;NNode > CurNode ->
			new_leader;
		NEpoch == CurEpoch,NZxid == CurZxid,NNode == CurNode ->
			new_leader;
		true ->
			self
	end.

send_election_ack(Node,#notification{proposed_leader=LeaderNode,ecount=ECount}=NewVote) ->
%% 	?DEBUG_F("~p -- ~p send election ack:~p to node:~p~n",[?MODULE,node(),NewVote,Node]),
%% 	election_msg_sender:send_vote_ack_to_node(Node, NewVote).
	case net_adm:ping(LeaderNode) of
		pong ->
			?DEBUG_F("~p -- ~p send election ack:~p to node:~p~n",[?MODULE,node(),NewVote,Node]),
			election_msg_sender:send_vote_ack_to_node(Node, NewVote);
		pang ->
			gen_fsm:send_event(?MODULE,{election_failed,ECount,{unlive_leader,LeaderNode}})
	end.

change_to_following_state(Timer,NNode,Epoch,VoteECount,Node,NEpoch,NLeader,NZxid,Zxid,State,ECount,CurEpoch,CurZxid,LeaderNode) ->
	cancel_timer(Timer),
	?INFO_F("~p -- self:~p change to [follower].~n",[?MODULE,node()]),
	send_election_ack(NNode,#notification{epoch=Epoch,ecount=VoteECount,node=Node,proposed_epoch=CurEpoch,proposed_leader=LeaderNode,proposed_zxid=CurZxid,zxid=Zxid}),
	FollowerTimerOutTimer = gen_fsm:send_event_after(?DEFAULT_WAITING_TIMEOUT, {election_failed,ECount,following_timeout}),
	{next_state,?ELECTION_STATE_NAME_FOLLOWING, State#election{votes=[],proposed_epoch=NEpoch,proposed_leader=NLeader,timer=FollowerTimerOutTimer,
									 proposed_zxid=NZxid,followers=[],followers_acks=[]}}.

change_to_following_state_when_rev_ack(Timer,State,NEpoch,NNode,NZxid,ECount) ->
	cancel_timer(Timer),
	?INFO_F("~p -- self(ack):~p change to [follower].~n",[?MODULE,node()]),
	FollowerTimerOutTimer = gen_fsm:send_event_after(?DEFAULT_WAITING_TIMEOUT, {election_failed,ECount,following_timeout}),
	{next_state,?ELECTION_STATE_NAME_FOLLOWING, State#election{votes=[],proposed_epoch=NEpoch,proposed_leader=NNode,timer=FollowerTimerOutTimer,
									 proposed_zxid=NZxid,followers=[],followers_acks=[]}}.


%% if all zab nodes responsed,then statistics votes immediately.
predicate_votes(Votes0,Quorum,Timer,ECount) when length(Votes0)+1 >= Quorum->
	cancel_timer(Timer),
	gen_fsm:send_event(?MODULE,{statistics_votes,ECount}),
	ok;
predicate_votes(_,_,_,_) -> 
	ok.

predicate_leader_acks(Quorum,ACKS,Timer,ECount) when length(ACKS)+1 >= Quorum->
	cancel_timer(Timer),
	gen_fsm:send_event(?MODULE,{start_commit_leader,ECount}),
	ok;
predicate_leader_acks(_,_,_,_) -> 
	ok.


cancel_timer(undefined) -> ok;
cancel_timer(Timer) ->
	gen_fsm:cancel_timer(Timer).


filter_all_followers_except_self(Votes) ->
	filter_all_followers0(Votes,[]).

filter_all_followers0([],L) -> lists:delete(node(), L);
filter_all_followers0([{Node,_}|T],L) ->
	filter_all_followers0(T,[Node|L]).


filter_ack_followers(ACKS) ->
	filter_ack_followers0(ACKS,[]).
filter_ack_followers0([],L) -> L;
filter_ack_followers0([{Node,_,_}|T],L) ->
	filter_ack_followers0(T, [Node|L]).


notify_all_followers_to_commit(Followers) ->
	election_msg_sender:notify_all_followers_to_commit(Followers).

%% 
%% check_new_state(#election{epoch=Epoch,node=Node,zxid=Zxid,ecount=ECount,
%% 					  proposed_zxid=CurZxid,proposed_epoch=CurEpoch,proposed_leader=LeaderNode}=OldState) ->
%% 	LiveNodes = zab_util:get_all_live_nodes(),
%% 	case lists:member(LeaderNode, LiveNodes) of
%% 		true ->
%% 			OldState;
%% 		false ->
%% 			OldState#election{proposed_zxid=Zxid,proposed_epoch=Epoch,proposed_leader=Node}
%% 	end.


