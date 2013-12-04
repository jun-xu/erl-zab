%% Author: sunshine
%% Created: 2012-7-24
%% Description: TODO: Add description to file_txn_log_SUITE
-module(file_txn_log_SUITE).

%%
%% Include files
%%
-include("log.hrl").
-include("txnlog.hrl").
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
	Config.

end_per_suite(_Config) ->
	{ok,Dir} = file:get_cwd(),
	os:cmd("rm -rf "++Dir++"/log.*"),
	timer:sleep(500),
	ok.

init_per_testcase(_TestCase, Config) ->
	{ok,Dir} = file:get_cwd(),
	os:cmd("rm -rf "++Dir++"/log.*"),
	timer:sleep(500),
	Config.

end_per_testcase(_TestCase, _Config) ->
	
	ok.

all() ->
	[
	 test_trunc_last_zxid,
	 test_load_logs1,
	 test_get_value,
	 test_all
	
	
%% 	 test_append_1000
	 ].

test_all(_) ->
	{ok,Root} = file:get_cwd(),
	Dir = Root ++ "/txn_log_test/",
	application:set_env(zab, txn_log_dir, Dir),
	{ok,_} = file_txn_log:start_link(),
	ok = file_txn_log:stop(test),
	%%--------------------------------------------------------------
	{ok,_} = file_txn_log:start_link(),
	ok = file_txn_log:append(1, <<1:32>>),
	{ok,1} = file_txn_log:get_last_zxid(),
	{ok,{1,<<1:32>>}} = file_txn_log:get_last_msg(),
	{error,ignored} = file_txn_log:append(0, <<0:32>>),
	ok = file_txn_log:stop(test),
	%%--------------- test rolllog when index num overflow----------------------------------
	{ok,_} = file_txn_log:start_link(),
	ok = file_txn_log:append(2, <<2:32>>),
	{ok,{2,<<2:32>>}} = file_txn_log:get_last_msg(),
	ok = file_txn_log:append(3, <<0:(1024*8*50)>>),
	{ok,{3,<<0:(1024*8*50)>>}} = file_txn_log:get_last_msg(),
	ok = file_txn_log:stop(test),
	F0 = filename:join([Dir,"log.0"]),
	F3 = filename:join([Dir,"log.3"]),
	{ok,#file_info{size = SF0}} = prim_file:read_file_info(F0),
	SF0 = 64*1024,
	{ok,#file_info{size = SF3}} = prim_file:read_file_info(F3),
	SF3 = 64*1024 - 32,
	{ok,FDlog0} = prim_file:open(filename:join([Dir,"log.0"]),[write,read,raw,binary]),
	{ok,<<_:32,_:(24*8),1:32,_:(24*8),2:32>>} = prim_file:read(FDlog0, 4+24+24+8),
				%%---test rolllog when data overflow------
	{ok,_} = file_txn_log:start_link(),
	{ok,3} = file_txn_log:get_last_zxid(),
	ok = file_txn_log:append(4, <<0:(50*8*1024)>>),
	ok = file_txn_log:stop(test),
	F4 = filename:join([Dir,"log.4"]),
	{ok,#file_info{size = SF4}} = prim_file:read_file_info(F4),
	SF4 = 64*1024 - 32,
	%%--------------------------------------------------------------
	{ok,_} = file_txn_log:start_link(),
	{ok,4} = file_txn_log:get_last_zxid(),
	ok = file_txn_log:stop(test),
	
	%%--------------------------------------------------------------
	{ok,FD0} = prim_file:open(filename:join([Dir,"log.5"]),[write,read,raw,binary]),
	prim_file:close(FD0),
	{ok,_} = file_txn_log:start_link(),
	{ok,4} = file_txn_log:get_last_zxid(),
	ok = file_txn_log:stop(test),
	false = filelib:is_file(filename:join([Dir,"log.5"])),
	%%--------------------------------------------------------------
	{ok,FD1} = prim_file:open(filename:join([Dir,"log.6"]),[write,read,raw,binary]),
	ok = prim_file:write(FD1, ?MAGIC_NUM),
	prim_file:close(FD1),
	{ok,_} = file_txn_log:start_link(),
	{ok,4} = file_txn_log:get_last_zxid(),
	ok = file_txn_log:stop(test),
	false = filelib:is_file(filename:join([Dir,"log.6"])),
	%%--------------------------------------------------------------
	{ok,FD2} = prim_file:open(filename:join([Dir,"log.7"]),[write,read,raw,binary]),
	ok = prim_file:write(FD2, ?MAGIC_NUM),
	{ok,_} = prim_file:position(FD2, 64*1024-64),
	ok = prim_file:write(FD2, <<"test">>),
	prim_file:close(FD2),
	{ok,_} = file_txn_log:start_link(),
	{ok,4} = file_txn_log:get_last_zxid(),
	{ok,{4,<<0:(50*8*1024)>>}} = file_txn_log:get_last_msg(),
	ok = file_txn_log:stop(test),
	false = filelib:is_file(filename:join([Dir,"log.7"])),
	%%--------------------------------------------------------------
	os:cmd("rm -rf "++Dir),
	ok.

test_append_1000(_) ->
	{ok,Root} = file:get_cwd(),
	Dir = Root ++ "/txn_log_test/",
	application:set_env(zab, txn_log_dir, Dir),
	{ok,_} = file_txn_log:start_link(),
	ST = zab_util:tstamp(),
	ok = append_times(1000),
	ET = zab_util:tstamp(),
	?INFO("cost time:~p~n",[(ET-ST)]),
	ok = file_txn_log:stop(test),
	ok.


test_trunc_last_zxid(_) ->
	{ok,Root} = file:get_cwd(),
	Dir = Root ++ "/txn_log_test/",
	application:set_env(zab, txn_log_dir, Dir),
	{ok,_} = file_txn_log:start_link(),
	ok = file_txn_log:truncate_last_zxid(),
	{ok,?UNDEFINED_ZXID} = file_txn_log:get_last_zxid(),
	
	file_txn_log:append(17, <<"hello">>),
	{ok,17} = file_txn_log:get_last_zxid(),
	ok = file_txn_log:truncate_last_zxid(),
	{ok,?UNDEFINED_ZXID} = file_txn_log:get_last_zxid(),
	
	
	file_txn_log:append(17, <<"hello">>),
	file_txn_log:append(18, <<"world">>),
	{ok,18} = file_txn_log:get_last_zxid(),
	ok = file_txn_log:truncate_last_zxid(),
	{ok,17} = file_txn_log:get_last_zxid(),
	
	ok = file_txn_log:stop(test),
	os:cmd("rm -rf "++Dir),
	ok.
test_load_logs1(_) ->
	{ok,Root} = file:get_cwd(),
	Dir = Root ++ "/txn_log_test/",
	application:set_env(zab, txn_log_dir, Dir),
	{ok,_} = file_txn_log:start_link(),
	
	{ok,[]} = file_txn_log:load_logs(0),
	file_txn_log:append(1, <<"hello">>),
	file_txn_log:append(2, <<"hello">>),
	{ok,[{1,<<"hello">>},{2,<<"hello">>}]} = file_txn_log:load_logs(0),
	{ok,[{1,<<"hello">>},{2,<<"hello">>}]} = zab:load_msgs_from(0),
	
	{ok,[{2,<<"hello">>}]} = file_txn_log:load_logs(1),
	{ok,[]} = file_txn_log:load_logs(2),
	
	file_txn_log:append(3, <<"hello">>),
	file_txn_log:append(4, <<"hello">>),	
	{ok,[{2,<<"hello">>},{3, <<"hello">>},{4, <<"hello">>}]} = file_txn_log:load_logs(1),
	{ok,[{2,<<"hello">>}]} = file_txn_log:load_logs(1, 1),
	{ok,[{2,<<"hello">>},{3, <<"hello">>}]} = file_txn_log:load_logs(1, 2),
	{ok,[{2,<<"hello">>},{3, <<"hello">>},{4, <<"hello">>}]} = file_txn_log:load_logs(1,10),
	{ok,[{3, <<"hello">>},{4, <<"hello">>}]} = file_txn_log:load_logs(2),

	{ok,[{4, <<"hello">>}]} = file_txn_log:load_logs(3),
	
	{ok,[]} = file_txn_log:load_logs(4),
	
	{error,not_found} = file_txn_log:load_logs(5),
	
	ok = file_txn_log:stop(test),
	os:cmd("rm -rf "++Dir),
	ok.

test_get_value(_) ->
	{ok,Root} = file:get_cwd(),
	Dir = Root ++ "/txn_log_test/",
	application:set_env(zab, txn_log_dir, Dir),
	application:set_env(zab, txn_log_dir, Dir),
	{ok,_} = file_txn_log:start_link(),
	
	file_txn_log:append(1, <<"hello">>),
	file_txn_log:append(2, <<"hello">>),
	
	{error,not_found} = file_txn_log:get_value(0),
	{ok,<<"hello">>} = file_txn_log:get_value(1),
	
	ok = file_txn_log:stop(test),
	os:cmd("rm -rf "++Dir),
	ok.	

append_times(0) -> ok;
append_times(T) ->
	ok = file_txn_log:append(1001-T, <<0:(8*1024)>>),
	append_times(T - 1).

