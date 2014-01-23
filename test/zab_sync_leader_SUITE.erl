%% Author: sunshine
%% Created: 2013-8-19
%% Description: TODO: Add description to zab_sync_leader_SUITE
-module(zab_sync_leader_SUITE).
%
%% Include files
%%
-include("log.hrl").
-include("txnlog_ex.hrl").
-include_lib("kernel/include/file.hrl").


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
	?INFO("start zab_sync_leader_SUITE"),
	Config.

end_per_suite(_Config) ->
	{ok,Dir} = file:get_cwd(),
	os:cmd("rm -rf "++Dir++"/log.*"),
	timer:sleep(500),
	ok.

init_per_testcase(_TestCase, Config) ->
	{ok,Dir} = file:get_cwd(),
	application:set_env(zab, txn_log_dir, "./"),
	os:cmd("rm -rf "++Dir++"/log.*"),
	timer:sleep(500),
	Config.

end_per_testcase(_TestCase, _Config) ->
	
	ok.

all() ->[
	test_commit_quorum_1
%% 	test_commit_quorum_3
	 ].

test_commit_quorum_3(_) ->
	{ok,_} = math_apply:start_link(),
	{ok,_} = file_txn_log_ex:start_link(),
	{ok,_} = zab_apply_server:start_link(),
	ok = zab_apply_server:reset_callback(math_apply, call),
	{ok,Leader} = zab_sync_leader:start_link(3),
	{ok,F1} = zab_follower_test:start_link(Leader,'n1@127.0.0.1',0,0),
	{ok,F2} = zab_follower_test:start_link(Leader,'n2@127.0.0.1',0,0),
	ok = zab_follower_test:register(F1),
	ok = zab_follower_test:register(F2),
	timer:sleep(2000),
	1=2,
	ok.


test_commit_quorum_1(_) ->
	{ok,_} = math_apply:start_link(),
	{ok,_} = file_txn_log_ex:start_link(),
	{ok,_} = zab_apply_server:start_link(),
	ok = zab_apply_server:reset_callback(math_apply, call),
	{ok,Leader} = zab_sync_leader:start_link(1),
%% 	test math: ((1+2) * 3 -1) / 4.
	ok = zab_sync_leader:sync(term_to_binary({reset,1}), self()),		
	ok = receive_ok(),
	ok = zab_sync_leader:sync(term_to_binary({plus,2}), self()),
	ok = receive_ok(),
	ok = zab_sync_leader:sync(term_to_binary({times,3}), self()),
	ok = receive_ok(),
	ok = zab_sync_leader:sync(term_to_binary({minus,1}), self()),
	ok = receive_ok(),
	ok = zab_sync_leader:sync(term_to_binary({divide,4}), self()),
	ok = receive_ok(),
	timer:sleep(1000),
	{2,4,1} = math_apply:result(),
	zab_sync_leader:stop(normal),
	zab_apply_server:stop(normal),
	file_txn_log_ex:stop(normal),
	math_apply:stop(),
	ok.


receive_ok() ->
	receive
		{ok,Zxid} -> ok
	after 1000 ->
		exit(test_failed)
		
	end.





