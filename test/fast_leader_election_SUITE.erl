%% Author: sunshine
%% Created: 2012-8-24
%% Description: TODO: Add description to fast_leader_election_SUITE
-module(fast_leader_election_SUITE).

%%
%% Include files
%%
-include("log.hrl").
-include("txnlog.hrl").
-include("zab.hrl").

%%
%% Exported Functions
%%
-export([]).

-compile(export_all).

%%
%% API Functions
%%
suite() ->
	[].

init_per_suite(Config) ->
	Config.

end_per_suite(_Config) ->
	ok.

init_per_testcase(_TestCase, Config) ->
	Config.
end_per_testcase(_TestCase, _Config) ->
	ok.

all() ->
	[
	test_total_order_predicate
%% 	test_election_leader_with_self_is_leader,
%% 	test_election_leader_with_self_is_leader2,
%% 	test_election_leader_with_self_is_follower,
%% 	test_election_leader_with_self_is_follower1,
%% 	test_election_leader_error_by_zabnodes_less_then_quorum,
%% 	test_election_votes_not_enough
	 ].

test_election_votes_not_enough(_) ->
	
	SelfNode = node(),
	ZabNodes = [SelfNode,'azab1@127.0.0.1','azab2@127.0.0.1'],
	Quorum = 3,
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok,Dir} = file:get_cwd(),
	RootDir = Dir ++ "/zab0/",
	
	erlang:set_cookie(node(), Cookie),
	?INFO("~p -- self cookie:~p~n",[?MODULE,erlang:get_cookie()]),
	{ok, Pwd} = file:get_cwd(),
	application:set_env(zab,txn_log_dir,Dir ++ "/zab0/"),
	application:start(ce),
	zab_sup:start_link(),
	zab_manager:start_zab(ZabNodes, 3,self()),	
	zab_manager:start_zab(ZabNodes, 3,self()),	
	%---------------------------------------------
	
	%%---------------------------------------------
	timer:sleep(1000*15),
	{ok,?ELECTION_STATE_NAME_LOOKING} = fast_leader_election_impl:get_state(),
	fast_leader_election_impl:stop(),
	ok.

test_election_leader_error_by_zabnodes_less_then_quorum(_) ->
	{ok,Dir} = file:get_cwd(),
	application:set_env(zab,txn_log_dir,Dir ++ "/zab0/"),
	application:start(ce),
	zab_sup:start_link(),
	zab_manager:start_zab([], 3,self()),	
	timer:sleep(500),
	{ok,?ELECTION_STATE_NAME_LOOKING} = fast_leader_election_impl:get_state(),
	ok.

test_total_order_predicate(_) ->
	self = fast_leader_election_impl:total_order_predicate('1',1,1,'2',2,2),
	new_leader = fast_leader_election_impl:total_order_predicate('1',3,1,'2',2,2),
	
	self = fast_leader_election_impl:total_order_predicate('1',1,1,'2',1,2),
	new_leader = fast_leader_election_impl:total_order_predicate('1',1,3,'2',1,2),
	
	self = fast_leader_election_impl:total_order_predicate('1',1,1,'2',1,1),
	new_leader = fast_leader_election_impl:total_order_predicate('3',1,1,'2',1,1),

	ok.


test_election_leader_with_self_is_leader2(_) ->
%% 	process_flag(trap_exit, true),
	%%--- start slave-----------------------------
	SelfNode = node(),
	ZabNodes = [SelfNode,'azab1@127.0.0.1','azab2@127.0.0.1','unline@sdfsd'],
	Quorum = 2,
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok,Dir} = file:get_cwd(),
	RootDir = Dir ++ "/zab0/",
	
	erlang:set_cookie(node(), Cookie),
	?INFO("~p -- self cookie:~p~n",[?MODULE,erlang:get_cookie()]),
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,azab1,Args),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Dir ++ "/zab1/"]),
	rpc:call(N1, application, set_env,[zab,quorum,3]),
	rpc:call(N1, application, start,[zab]),
	timer:sleep(1000*1),
	{ok,N2} = slave:start(Host,azab2,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Dir ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,quorum,3]),
	rpc:call(N2, application, start,[zab]),
	timer:sleep(1000*1),
	application:set_env(zab,txn_log_dir,Dir ++ "/zab0/"),
	application:start(ce),
	zab_sup:start_link(),
	zab_manager:start_zab(ZabNodes, 3,self()),	
	%---------------------------------------------
	
	%%---------------------------------------------
	assert_msg({election,leader,['azab1@127.0.0.1','azab2@127.0.0.1']}),
	{ok,{?SERVER_STATE_FOLLOWING,[],SelfNode}} = rpc:call(N1, zab_manager, get_state,[]),
	{ok,{?SERVER_STATE_FOLLOWING,[],SelfNode}} = rpc:call(N2, zab_manager, get_state,[]),
	slave:stop(N1),
	slave:stop(N2),
	ok.

test_election_leader_with_self_is_leader(_) ->
%% 	process_flag(trap_exit, true),
	%%--- start slave-----------------------------
	SelfNode = node(),
	ZabNodes = [SelfNode,'azab1@127.0.0.1','azab2@127.0.0.1'],
	Quorum = 2,
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok,Dir} = file:get_cwd(),
	RootDir = Dir ++ "/zab0/",
	
	erlang:set_cookie(node(), Cookie),
	?INFO("~p -- self cookie:~p~n",[?MODULE,erlang:get_cookie()]),
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,azab1,Args),
	rpc:call(N1, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Dir ++ "/zab1/"]),
	rpc:call(N1, application, set_env,[zab,quorum,3]),
	rpc:call(N1, application, start,[zab]),
	timer:sleep(1000*1),
	{ok,N2} = slave:start(Host,azab2,Args),
	rpc:call(N2, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Dir ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,quorum,3]),
	rpc:call(N2, application, start,[zab]),
	timer:sleep(1000*1),
	application:set_env(zab,txn_log_dir,Dir ++ "/zab0/"),
	application:start(ce),
	zab_sup:start_link(),
	zab_manager:start_zab(ZabNodes, 3,self()),	
	zab_manager:start_zab(ZabNodes, 3,self()),	
	%---------------------------------------------
	
	%%---------------------------------------------
	assert_msg({election,leader,['azab1@127.0.0.1','azab2@127.0.0.1']}),
	{ok,{?SERVER_STATE_FOLLOWING,[],SelfNode}} = rpc:call(N1, zab_manager, get_state,[]),
	{ok,{?SERVER_STATE_FOLLOWING,[],SelfNode}} = rpc:call(N2, zab_manager, get_state,[]),
	slave:stop(N1),
	slave:stop(N2),
	ok.


test_election_leader_with_self_is_follower(_) ->
	SelfNode = node(),
	ZabNodes = [SelfNode,'zzz@127.0.0.1','azab1@127.0.0.1'],
	Quorum = 3,
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok,Dir} = file:get_cwd(),
	RootDir = Dir ++ "/zab0/",
	
	erlang:set_cookie(node(), Cookie),
	?INFO("~p -- self cookie:~p~n",[?MODULE,erlang:get_cookie()]),
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,azab1,Args),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Dir ++ "/zab1/"]),
	rpc:call(N1, application, set_env,[zab,quorum,3]),
	rpc:call(N1, application, start,[zab]),
	timer:sleep(1000*1),
	{ok,N2} = slave:start(Host,zzz,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Dir ++ "/zzz/"]),
	rpc:call(N2, application, set_env,[zab,quorum,3]),
	rpc:call(N2, application, start,[zab]),
	timer:sleep(1000*1),
	application:set_env(zab,txn_log_dir,Dir ++ "/zab0/"),
	application:start(ce),
	zab_sup:start_link(),
	application:set_env(zab,quorum, 3),
%% %% 	fast_leader_election_impl:start_election(ZabNodes,Quorum),
	rpc:call(N2, zab_manager, start_zab,[ZabNodes, 3]),
	zab_manager:start_zab(ZabNodes, Quorum,self()),	
	%---------------------------------------------
	
	%%---------------------------------------------
	assert_msg({election,follower,'zzz@127.0.0.1'}),
	timer:sleep(1000),
	{ok,{?SERVER_STATE_FOLLOWING,[],'zzz@127.0.0.1'}} = rpc:call(N1, zab_manager, get_state,[]),
	{ok,{?SERVER_STATE_LEADING,['azab1@127.0.0.1',SelfNode],'zzz@127.0.0.1'}} = rpc:call(N2, zab_manager, get_state,[]),
	
	slave:stop(N1),
	slave:stop(N2),
	
	ok.


test_election_leader_with_self_is_follower1(_) ->
	SelfNode = node(),
	ZabNodes = [SelfNode,'zzz1@127.0.0.1','zzz@127.0.0.1'],
	Quorum = 2,
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok,Dir} = file:get_cwd(),
	RootDir = Dir ++ "/zab0/",
	
	erlang:set_cookie(node(), Cookie),
	?INFO("~p -- self cookie:~p~n",[?MODULE,erlang:get_cookie()]),
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,azab1,Args),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Dir ++ "/zab1/"]),
	rpc:call(N1, application, set_env,[zab,quorum,3]),
	rpc:call(N1, application, start,[zab]),
	timer:sleep(1000*1),
	{ok,N2} = slave:start(Host,zzz,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Dir ++ "/zzz/"]),
	rpc:call(N2, application, set_env,[zab,quorum,2]),
	rpc:call(N2, application, start,[zab]),
	timer:sleep(1000*1),
	application:set_env(zab,txn_log_dir,Dir ++ "/zab0/"),
	application:start(ce),
	zab_sup:start_link(),
	application:set_env(zab,quorum, 3),
%% %% 	fast_leader_election_impl:start_election(ZabNodes,Quorum),
	zab_manager:start_zab(ZabNodes, Quorum,self()),	
	%---------------------------------------------
	
	%%---------------------------------------------
	assert_msg({election,follower,'zzz@127.0.0.1'}),
	timer:sleep(1000),
	{ok,{?SERVER_STATE_LOOKING,[],undefined}} = rpc:call(N1, zab_manager, get_state,[]),
	{ok,{?SERVER_STATE_LEADING,[SelfNode],'zzz@127.0.0.1'}} = rpc:call(N2, zab_manager, get_state,[]),
	
	slave:stop(N1),
	slave:stop(N2),
	
	ok.

assert_msg(Msg) ->
	receive 
		 Msg ->
			 ok;
		 O -> 
			 ?INFO("~p -- unexcept msg:~p~n",[?MODULE,O]),
			 assert_msg(Msg)
	after 1000*6 ->
			exit(test_failed)
	end.






