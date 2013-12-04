%% Author: clues
%% Created: Apr 1, 2013
%% Description: TODO: Add description to zab_cluster_SUITE
-module(zab_integration_SUITE).

%%
%% Include files
%%
-include("log.hrl").
-include("txnlog.hrl").
-include("zab.hrl").

-define(TEST_BIN,<<"this is jack,i want test myself with zab protocol,id:4017,sex:male,age:17">>).

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
	{ok, Pwd} = file:get_cwd(),
	os:cmd("rm -rf "++ Pwd ++"/zab*"),
	os:cmd("rm -rf "++Pwd++"/last_commit_zxid.log"),
	application:set_env(zab, txn_log_dir, "./"),
	application:set_env(zab, known_nodes, []),
	timer:sleep(1000),
	ok.

init_per_testcase(_TestCase, Config) ->
	{ok, Pwd} = file:get_cwd(),
	os:cmd("rm -rf "++ Pwd ++"/zab*"),
	os:cmd("rm -rf "++Pwd++"/last_commit_zxid.log"),
	Config.
end_per_testcase(_TestCase, _Config) ->
	ok.

all() ->
	[
	 test_election_ok,
  	 test_election_all,
	 test_election_pre_leader_down,
	 test_election_vote_ack_not_enough,
	 test_recover_trunc_leader,
	 test_recover_trunc_follower,
	 test_recover_diff,
	 test_error_lost_quorum,
	 test_error_lost_leader,
	 test_error_leader_checked_timeout,
	 test_single_node
	
	
	
%% 	 stress_test

%% 	 test_single_node_performance

	 ].

test_election_ok(_) ->
%% 	process_flag(trap_exit, true),
	%%--- start slave-----------------------------
	SelfNode = node(),
	ZabNodes = ['zab1@127.0.0.1','zab2@127.0.0.1','zab3@127.0.0.1'],
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,zab1,Args),
	rpc:call(N1, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab1/"]),
	rpc:call(N1, application, start,[zab]),
	
	{ok,N2} = slave:start(Host,zab2,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N2, application, start,[zab]),
	

	{ok,N3} = slave:start(Host,zab3,Args),
	rpc:call(N3, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab3/"]),
	rpc:call(N3, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N3, application, start,[zab]),
	timer:sleep(1000*2),	
	%---------------------------------------------
	
	%%---------------------------------------------
	
	{ok,{?SERVER_STATE_FOLLOWING,[],'zab2@127.0.0.1'}} = rpc:call(N1, zab, get_state,[]),
	{ok,{?SERVER_STATE_LEADING,_,'zab2@127.0.0.1'}} = rpc:call(N2, zab, get_state,[]),
	{ok,{?SERVER_STATE_FOLLOWING,[],'zab2@127.0.0.1'}} = rpc:call(N3, zab, get_state,[]),

	
	slave:stop(N1),
	slave:stop(N2),
	slave:stop(N3),
	ok.

test_election_all(_) ->
	SelfNode = node(),
	ZabNodes = ['zab1@127.0.0.1','zab2@127.0.0.1','zab3@127.0.0.1'],
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,zab1,Args),
	rpc:call(N1, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab1/"]),
	rpc:call(N1, application, start,[zab]),
	
	{ok,N2} = slave:start(Host,zab2,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N2, application, start,[zab]),
	timer:sleep(2*1000),
	
	{ok,{?SERVER_STATE_FOLLOWING,[],'zab2@127.0.0.1'}} = rpc:call(N1, zab, get_state,[]),
	{ok,{?SERVER_STATE_LEADING,_,'zab2@127.0.0.1'}} = rpc:call(N2, zab, get_state,[]),	
	
	%%stop one,leader will lost quorum
	rpc:call(N1, application, stop, [zab]),
	timer:sleep(1*1000),
	{ok,{?SERVER_STATE_LOOKING,[],undefined}} = rpc:call(N2, zab, get_state,[]),
	
	%%repeat restart N1,will election N2 is leader
	rpc:call(N1, application, start,[zab]),
	timer:sleep(6*1000),
	
	{ok,{?SERVER_STATE_FOLLOWING,[],'zab2@127.0.0.1'}} = rpc:call(N1, zab, get_state,[]),
	{ok,{?SERVER_STATE_LEADING,_,'zab2@127.0.0.1'}} = rpc:call(N2, zab, get_state,[]),		
	
%% 	%%add N3,N2 is leader
	{ok,N3} = slave:start(Host,zab3,Args),
	rpc:call(N3, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab3/"]),
	rpc:call(N3, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N3, application, start,[zab]),
	timer:sleep(2*1000),	
	%---------------------------------------------
	
	%%---------------------------------------------
	
	{ok,{?SERVER_STATE_FOLLOWING,[],'zab2@127.0.0.1'}} = rpc:call(N1, zab, get_state,[]),
	{ok,{?SERVER_STATE_LEADING,_,'zab2@127.0.0.1'}} = rpc:call(N2, zab, get_state,[]),
	{ok,{?SERVER_STATE_FOLLOWING,[],'zab2@127.0.0.1'}} = rpc:call(N3, zab, get_state,[]),


	%%stop leader,cluster lost leader
	rpc:call(N2, application, stop, [zab]),
	timer:sleep(3*1000),
	{ok,{?SERVER_STATE_FOLLOWING,[],'zab3@127.0.0.1'}} = rpc:call(N1, zab, get_state,[]),
	{ok,{?SERVER_STATE_LEADING,_,'zab3@127.0.0.1'}} = rpc:call(N3, zab, get_state,[]),
	
	{ok,_} = rpc:call(N1, zab, write, [{plus,1,1}]),
	{ok,_} = rpc:call(N3, zab, write, [{plus,2,2}]),
	slave:stop(N1),
	slave:stop(N2),
	slave:stop(N3),
	ok.


test_recover_trunc_leader(_) ->
	SelfNode = node(),
	ZabNodes = ['zab1@127.0.0.1','zab2@127.0.0.1','zab3@127.0.0.1'],
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,zab1,Args),
	rpc:call(N1, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab1/"]),
	rpc:call(N1, application, start,[zab]),
	rpc:call(N1,file_txn_log,append,[1,<<>>]),
	{ok,1} = rpc:call(N1, zab, get_last_zxid,[]),
	
	{ok,N2} = slave:start(Host,zab2,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N2, application, start,[zab]),
	
	{ok,N3} = slave:start(Host,zab3,Args),
	rpc:call(N3, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab3/"]),
	rpc:call(N3, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N3, application, start,[zab]),	
	timer:sleep(5*1000),
	
	{ok,{?SERVER_STATE_LEADING,_,'zab1@127.0.0.1'}} = rpc:call(N1, zab, get_state,[]),
	{ok,{?SERVER_STATE_FOLLOWING,_,'zab1@127.0.0.1'}} = rpc:call(N2, zab, get_state,[]),
	{ok,{?SERVER_STATE_FOLLOWING,_,'zab1@127.0.0.1'}} = rpc:call(N3, zab, get_state,[]),
	timer:sleep(2*1000),
	{ok,0} = rpc:call(N1, zab, get_last_zxid,[]),
	{ok,0} = rpc:call(N3, zab, get_last_zxid,[]),

	slave:stop(N1),
	slave:stop(N2),
	slave:stop(N3),
	ok.

test_recover_trunc_follower(_) ->
	?INFO("start test_recover_trunc_follower"),
	SelfNode = node(),
	ZabNodes = ['zab1@127.0.0.1','zab2@127.0.0.1','zab3@127.0.0.1'],
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,zab1,Args),
	rpc:call(N1, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab1/"]),
	rpc:call(N1, application, start,[zab]),
	
	{ok,N2} = slave:start(Host,zab2,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N2, application, start,[zab]),
	
	{ok,N3} = slave:start(Host,zab3,Args),
	rpc:call(N3, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab3/"]),
	rpc:call(N3, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N3, application, start,[zab]),		
	timer:sleep(2000),
	
	{ok,LastZxid} = rpc:call(N1, zab, get_last_zxid,[]),
	NewZxid = LastZxid+1,
	rpc:call(N1,file_txn_log,append,[NewZxid,<<>>]),
	{ok,NewZxid} = rpc:call(N1, zab, get_last_zxid,[]),
	
	
	
	slave:stop(N1),
	
	{ok,N11} = slave:start(Host,zab1,Args),
	rpc:call(N11, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N11, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab1/"]),
	rpc:call(N11, application, start,[zab]),	
	timer:sleep(2000),
	{ok,LastZxid} = rpc:call(N11, zab, get_last_zxid,[]),

	slave:stop(N11),
	slave:stop(N3),
	slave:stop(N2),
	?INFO("end test_recover_trunc_follower"),
	ok.

test_recover_diff(_) ->
	SelfNode = node(),
	ZabNodes = ['zab1@127.0.0.1','zab2@127.0.0.1','zab3@127.0.0.1'],
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,zab1,Args),
	rpc:call(N1, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab1/"]),
	rpc:call(N1, application, start,[zab]),
	
	{ok,N2} = slave:start(Host,zab2,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N2, application, start,[zab]),
	
	timer:sleep(3000),
	
	{ok,Zxid1} = rpc:call(N1,zab,write,["hello"]),
	{ok,Zxid2} = rpc:call(N2,zab,write,["world"]),
	
	{ok,N3} = slave:start(Host,zab3,Args),
	rpc:call(N3, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab3/"]),
	rpc:call(N3, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N3, application, start,[zab]),		
	timer:sleep(1000),
	
	{ok,"hello"} = rpc:call(N3, zab, get_value,[Zxid1]),
	{ok,"world"} = rpc:call(N3, zab, get_value,[Zxid2]),

	slave:stop(N1),
	slave:stop(N3),
	slave:stop(N2),
	ok.

%%when in election a higher node down,will create a dirty vote
%%this vote cause no leader will be selected.
test_election_pre_leader_down(_) ->
	SelfNode = node(),
	ZabNodes = ['zab1@127.0.0.1','zab2@127.0.0.1','zab3@127.0.0.1','zab4@127.0.0.1'],
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
	{ok,N1} = slave:start(Host,zab1,Args),
	rpc:call(N1, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab1/"]),
	rpc:call(N1, application, start,[zab]),
	
	
	{ok,N4} = slave:start(Host,zab4,Args),
	rpc:call(N4, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab4/"]),
	rpc:call(N4, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N4, application, start,[zab]),	
	timer:sleep(2000),
	%% N4 will be a dirty vote,unless you restart N4
	slave:stop(N4),
	
	{ok,N2} = slave:start(Host,zab2,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N2, application, start,[zab]),
	
	{ok,N3} = slave:start(Host,zab3,Args),
	rpc:call(N3, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab3/"]),
	rpc:call(N3, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N3, application, start,[zab]),	
	timer:sleep(5*1000),
	
	{ok,{?SERVER_STATE_LEADING,_,'zab3@127.0.0.1'}} = rpc:call(N3, zab, get_state,[]),

	slave:stop(N1),
	slave:stop(N2),
	slave:stop(N3),
	ok.

test_election_vote_ack_not_enough(_) ->
	SelfNode = node(),
	ZabNodes = ['zab1@127.0.0.1','zab2@127.0.0.1','zab3@127.0.0.1'],
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
	{ok,N1} = slave:start(Host,zab1,Args),
	rpc:call(N1, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab1/"]),
	rpc:call(N1, application, start,[zab]),
	
	timer:sleep(3*1000),
	
	{ok,{?SERVER_STATE_LOOKING,[],'undefined'}} = rpc:call(N1, zab, get_state,[]),

	slave:stop(N1),
	ok.
test_error_lost_quorum(_) ->
	SelfNode = node(),
	ZabNodes = ['zab1@127.0.0.1','zab2@127.0.0.1','zab3@127.0.0.1'],
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,zab1,Args),
	rpc:call(N1, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab1/"]),
	rpc:call(N1, application, start,[zab]),
	
	{ok,N2} = slave:start(Host,zab2,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N2, application, start,[zab]),
	timer:sleep(2*1000),
	{ok,N3} = slave:start(Host,zab3,Args),
	rpc:call(N3, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab3/"]),
	rpc:call(N3, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N3, application, start,[zab]),
	
	timer:sleep(1*1000),
	{ok,{?SERVER_STATE_FOLLOWING,_,'zab2@127.0.0.1'}} = rpc:call(N1, zab, get_state,[]),
	{ok,{?SERVER_STATE_LEADING,_,'zab2@127.0.0.1'}} = rpc:call(N2, zab, get_state,[]),
	{ok,{?SERVER_STATE_FOLLOWING,_,'zab2@127.0.0.1'}} = rpc:call(N3, zab, get_state,[]),
	
	slave:stop(N1),
	timer:sleep(1000),
	{ok,{?SERVER_STATE_LEADING,_,'zab2@127.0.0.1'}} = rpc:call(N2, zab, get_state,[]),
	
	slave:stop(N3),
	timer:sleep(1000),
	{ok,{?SERVER_STATE_LOOKING,[],undefined}} = rpc:call(N2, zab, get_state,[]),
	slave:stop(N2),
	ok.

test_error_lost_leader(_) ->
	SelfNode = node(),
	ZabNodes = ['zab1@127.0.0.1','zab2@127.0.0.1','zab3@127.0.0.1'],
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,zab1,Args),
	rpc:call(N1, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab1/"]),
	rpc:call(N1, application, start,[zab]),
	
	{ok,N2} = slave:start(Host,zab2,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N2, application, start,[zab]),
	timer:sleep(2*1000),
	
	{ok,{?SERVER_STATE_FOLLOWING,_,'zab2@127.0.0.1'}} = rpc:call(N1, zab, get_state,[]),
	{ok,{?SERVER_STATE_LEADING,_,'zab2@127.0.0.1'}} = rpc:call(N2, zab, get_state,[]),
	
	slave:stop(N2),
	timer:sleep(1000),
	{ok,{?SERVER_STATE_LOOKING,[],'undefined'}} = rpc:call(N1, zab, get_state,[]),
	
	slave:stop(N1),
	ok.

test_error_leader_checked_timeout(_) ->
	SelfNode = node(),
	ZabNodes = ['zab1@127.0.0.1','zab2@127.0.0.1','zab3@127.0.0.1'],
	Cookie = zab_test,
	erlang:set_cookie(SelfNode, Cookie),
	
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,zab1,Args),
	rpc:call(N1, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab1/"]),
	rpc:call(N1, application, start,[zab]),
	rpc:call(N1,file_txn_log,append,[1,<<>>]),
	
	{ok,N2} = slave:start(Host,zab2,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Pwd ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,known_nodes,ZabNodes]),
	rpc:call(N2, application, start,[zab]),
	
	timer:sleep(5000),

	{error,_} = rpc:call(N1, zab, write,["hello"]),

	slave:stop(N1),
	slave:stop(N2),
	ok.

test_single_node(_) ->
%% 	process_flag(trap_exit, true),
	%%--- start slave-----------------------------
	application:start(zab),
	application:set_env(zab, apply_mod, {math_apply,call}),
	
	{ok,{?SERVER_STATE_LEADING,[],_}} = zab:get_state(),
	
	{ok,_} = zab:write({reset,1}),
	{ok,_} = zab:write({plus,3}),
	{ok,_} = zab:write({plus,3}),
	{ok,_} = zab:write({plus,3}),
	{ok,_} = zab:write({plus,3}),
	{ok,_} = zab:write({plus,3}),
	application:stop(zab),
	ok.

test_single_node_performance(_) ->
	application:start(zab),
	application:set_env(zab, apply_mod, {math_apply,call}),
	timer:sleep(500),
	{ok,{?SERVER_STATE_LEADING,[],_}} = zab:get_state(),
	Times = 20, 
	L = loop_create_client(Times, []),
	loop_monitor(L, 0, 0),
	timer:sleep(2000),
	ok.

loop_create_client(0,L) -> L;
loop_create_client(Time,L) ->
	Pid = spawn_link(?MODULE,loop_write_time,[10]),
	Ref = erlang:monitor(process, Pid),
	loop_create_client(Time-1,[{Ref,Pid}|L]).

loop_write_time(0) -> ok;
loop_write_time(Times) ->
	{ok,_} = zab:write({plus,1}),
	loop_write_time(Times - 1).	


loop_monitor([],S,F) -> {S,F};
loop_monitor(Pids,S,F) ->
	receive
		{'DOWN',Ref,process,_,normal} ->
			Pids0 = proplists:delete(Ref, Pids),
			loop_monitor(Pids0,S+1,F);
		{'DOWN',Ref,process,_,_} ->
			Pids0 = proplists:delete(Ref, Pids),
			loop_monitor(Pids0,S,F+1);
		_INFO ->
			loop_monitor(Pids,S,F)
	
	 after 15*1000 ->
			[{_,P}|_] = Pids,
			?INFO_F("~p -- not terminate pids:~p~n",[?MODULE,Pids]),
			exit(test_failed)
	end.


assert_msg(Msg) ->
	receive 
		 Msg ->
			 ok;
		 O -> 
			 ?INFO_F("~p -- unexcept msg:~p~n",[?MODULE,O]),
			 assert_msg(Msg)
	after 1000*6 ->
			exit(test_failed)
	end.


stress_test(_) ->
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
	?INFO_F("~p -- self cookie:~p~n",[?MODULE,erlang:get_cookie()]),
	{ok, Pwd} = file:get_cwd(),
	Host = '127.0.0.1',
	Args = "-pa "++ Pwd ++"/../../ebin "++Pwd ++"/../../deps/ce/ebin -setcookie " ++ atom_to_list(Cookie),
    {ok,N1} = slave:start(Host,azab1,Args),
	rpc:call(N1, application, set_env,[zab,txn_log_dir,Dir ++ "/zab1/"]),
	rpc:call(N1, application, set_env,[zab,quorum,Quorum]),
	rpc:call(N1, application, start,[zab]),
	timer:sleep(1000*1),
	
	{ok,N2} = slave:start(Host,azab2,Args),
	rpc:call(N2, application, set_env,[zab,txn_log_dir,Dir ++ "/zab2/"]),
	rpc:call(N2, application, set_env,[zab,quorum,Quorum]),
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
	timer:sleep(3*1000),

	T1 = zab_util:tstamp(),
	Max = 5000,
	lists:foreach(fun(_) ->
						  zab:write(?TEST_BIN) end, lists:seq(1, Max)),
	
	?INFO_F("~p write ~p msg,use time(ms):~p~n",[?MODULE,Max,zab_util:tstamp()-T1]),
	
	slave:stop(N1),
	slave:stop(N2),
	ok.	

