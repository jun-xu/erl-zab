%% Author: sunshine
%% Created: 2013-5-23
%% Description: TODO: Add description to zab_apply_server_SUITE
-module(zab_apply_server_SUITE).

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
	
	{ok,Dir} = file:get_cwd(),
	os:cmd("rm -rf "++Dir++"/log.*"),
	os:cmd("rm -rf "++Dir++"/last_commit_zxid.log"),
	timer:sleep(500),
	ok.

init_per_testcase(_TestCase, Config) ->
	?DEBUG("start zab apply server test"),
	application:set_env(zab, txn_log_dir, "./"),
	{ok,Dir} = file:get_cwd(),
	os:cmd("rm -rf "++Dir++"/log.*"),
	os:cmd("rm -rf "++Dir++"/last_commit_zxid.log"),
	timer:sleep(500),
	Config.

end_per_testcase(_TestCase, _Config) ->
	ok.

all() ->
	[
	
	test_common,
	test_other_commit,
	test_undefined_commit
	 ].

test_undefined_commit(_) ->
	math_apply:start_link(),
	application:set_env(?APP_NAME, apply_mod, {math_apply, call}),
	{ok,Dir} = file:get_cwd(),
	os:cmd("rm -rf "++Dir++"/log.*"),
	os:cmd("rm -rf "++Dir++"/last_commit_zxid.log"),
	timer:sleep(500),
	{ok,_} = file_txn_log:start_link(),
	%% math: ((1+2) * 3 -1) / 4.
	ok = file_txn_log:append(1, term_to_binary({reset,1})),
	ok = file_txn_log:append(2, term_to_binary({reset,1})),
	ok = file_txn_log:append(4, term_to_binary({plus,2})),
	{ok,Pid} = zab_apply_server:start_link(),
	ok = zab_apply_server:load_msg_to_commit(3),	
	ok = zab_apply_server:clear_callback(),
	ok = file_txn_log:append(5, term_to_binary({reset,1})),
	
	ok = zab_apply_server:load_msg_to_commit(5),
	ok = zab_apply_server:load_msg_to_commit(5),
	ok = zab_apply_server:load_msg_to_commit(6),
	{ok,5} = zab_apply_server:get_last_commit_zxid(),
	Pid ! test,
	gen_server:cast(Pid, test),
	file_txn_log:stop(normal),
	zab_apply_server:stop(normal),
	math_apply:stop(),
	ok.


test_common(_) ->
	math_apply:start_link(),
	
	{ok,_} = file_txn_log:start_link(),
	%% math: ((1+2) * 3 -1) / 4.
	ok = file_txn_log:append(1, term_to_binary({reset,1})),
	ok = file_txn_log:append(2, term_to_binary({reset,1})),
	ok = file_txn_log:append(4, term_to_binary({plus,2})),
	ok = file_txn_log:append(6, term_to_binary({times,3})),
	ok = file_txn_log:append(7, term_to_binary({minus,1})),
	ok = file_txn_log:append(8, term_to_binary({divide,4})),
	{ok,Pid} = zab_apply_server:start_link(),
	%% test when applymod is undefined. ignore.
	ok = zab_apply_server:commit(1,term_to_binary({reset,1})),
	ok = zab_apply_server:load_msg_to_commit(1),				 
%% ------------------ 	
	ok = zab_apply_server:reset_callback(math_apply, call),
	{ok,1} = zab_apply_server:get_last_commit_zxid(),
	ok = zab_apply_server:commit(2,term_to_binary({reset,1})),
	ok = zab_apply_server:load_msg_to_commit(5),
	ok = zab_apply_server:load_msg_to_commit(7),
	ok = zab_apply_server:commit(8,term_to_binary({divide,4})),
%% 	%% test when zxid lease then last commit id. ignore.
	ok = zab_apply_server:commit(1,term_to_binary({reset,1})),
	ok = zab_apply_server:load_msg_to_commit(1),	
	{ok,8} = zab_apply_server:get_last_commit_zxid(),
	%% some error test.
	?INFO("some error test"),
	ok = zab_apply_server:load_msg_to_commit(8),
	ok = zab_apply_server:load_msg_to_commit(9),
	{ok,8} = zab_apply_server:get_last_commit_zxid(),
	ok = zab_apply_server:stop(normal),
	{2,4,1} = math_apply:result(),
%% 	%------------ restart ----------------------
	
%% 	
	{ok,_} = zab_apply_server:start_link(),
	{ok,8} = zab_apply_server:get_last_commit_zxid(),
	ok = zab_apply_server:reset_callback(math_apply, call),
	ok = file_txn_log:append(9, term_to_binary({reset,1})),
	ok = zab_apply_server:commit(9,term_to_binary({reset,1})),
	ok = file_txn_log:append(10, term_to_binary({error,test})),
	ok = zab_apply_server:commit(10,term_to_binary({error,test})),
	ok = zab_apply_server:load_msg_to_commit(12),
	ok = file_txn_log:append(11, term_to_binary({reset,1})),
	ok = file_txn_log:append(12, term_to_binary({error,exit_pid_test})),   %% may stop.
	ok = zab_apply_server:load_msg_to_commit(12),   %% may stop.
	timer:sleep(800),
	{ok,_} = zab_apply_server:start_link(),
	ok = zab_apply_server:reset_callback(math_apply, call),
	ok = zab_apply_server:load_msg_to_commit(12),   %% may stop.
	timer:sleep(800),
	application:set_env(?APP_NAME, apply_mod, {math_apply, call}),
	{ok,_} = zab_apply_server:start_link(),
	{ok,11} = zab_apply_server:get_last_commit_zxid(),
	zab_apply_server:stop(normal),
	file_txn_log:stop(normal),
	math_apply:stop(),
	ok.


test_other_commit(_) ->
	{ok,_} = file_txn_log:start_link(),
	{ok,_} = zab_apply_server:start_link(),
	ok = zab_apply_server:load_msg_to_commit(22),
	{ok,0} = zab_apply_server:get_last_commit_zxid(),
	ok = zab_apply_server:reset_callback(math_apply, call),
	ok = zab_apply_server:load_msg_to_commit(22),
	{ok,0} = zab_apply_server:get_last_commit_zxid(),
	zab_apply_server:stop(normal),
	file_txn_log:stop(normal),
	ok.








