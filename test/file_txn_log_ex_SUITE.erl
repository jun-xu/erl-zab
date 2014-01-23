%% Author: sunshine
%% Created: 2014-1-15
%% Description: TODO: Add description to file_txn_log_ex
-module(file_txn_log_ex_SUITE).

%%
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
	 test_all,
	 test_trunc_last_zxid,
	 test_load_logs,
	 test_get_value
%% 	 test_loop_append
	 ].
test_get_value(_) ->
	{ok,Root} = file:get_cwd(),
	Dir = Root ++ "/txn_log_test/",
	application:set_env(zab, txn_log_dir, Dir),
	application:set_env(zab, txn_log_dir, Dir),
	{ok,_} = file_txn_log_ex:start_link(),
	
	file_txn_log_ex:append(1, <<"hello">>),
	file_txn_log_ex:append(2, <<"hello">>),
	
	{error,not_found} = file_txn_log_ex:get_value(0),
	{ok,<<"hello">>} = file_txn_log_ex:get_value(1),
	file_txn_log_ex:append(3,<<3:(512*8)>>),
	file_txn_log_ex:append(4,<<4:(512*8)>>),	
	file_txn_log_ex:append(5,<<5:(512*8)>>),
	file_txn_log_ex:append(6,<<6:(512*8)>>),
	{ok,<<3:(512*8)>>} = file_txn_log_ex:get_value(3),
	{ok,<<4:(512*8)>>} = file_txn_log_ex:get_value(4),
	{ok,<<5:(512*8)>>} = file_txn_log_ex:get_value(5),
	{ok,<<6:(512*8)>>} = file_txn_log_ex:get_value(6),
	{error,not_found} = file_txn_log_ex:get_value(7),
	ok = file_txn_log_ex:stop(test),
	os:cmd("rm -rf "++Dir),
	ok.

test_load_logs(_) ->
	{ok,Root} = file:get_cwd(),
	Dir = Root ++ "/txn_log_test/",
	application:set_env(zab, txn_log_dir, Dir),
	{ok,_} = file_txn_log_ex:start_link(),
	
	{ok,[]} = file_txn_log_ex:load_logs(0),
	file_txn_log_ex:append(1, <<"hello">>),
	file_txn_log_ex:append(2, <<"hello">>),
	{ok,[{1,<<"hello">>},{2,<<"hello">>}]} = file_txn_log_ex:load_logs(0),
%% 	{ok,[{1,<<"hello">>},{2,<<"hello">>}]} = zab:load_msgs_from(0),
	
	{ok,[{2,<<"hello">>}]} = file_txn_log_ex:load_logs(1),
	{ok,[]} = file_txn_log_ex:load_logs(2),
	B = <<0:(512*8)>>,
	file_txn_log_ex:append(3,B),
	file_txn_log_ex:append(4, B),	
	{ok,[{2,<<"hello">>},{3,B},{4, B}]} = file_txn_log_ex:load_logs(1),
	{ok,[{2,<<"hello">>}]} = file_txn_log_ex:load_logs(1, 1),
	{ok,[{2,<<"hello">>},{3,B}]} = file_txn_log_ex:load_logs(1, 2),
	{ok,[{2,<<"hello">>},{3,B},{4,B}]} = file_txn_log_ex:load_logs(1,10),
	{ok,[{3,B},{4,B}]} = file_txn_log_ex:load_logs(2),

	{ok,[{4, B}]} = file_txn_log_ex:load_logs(3),
	
	{ok,[]} = file_txn_log_ex:load_logs(4),
	{error,not_found} = file_txn_log_ex:load_logs(5),
	file_txn_log_ex:append(5, B),	
	file_txn_log_ex:append(6, B),	
	file_txn_log_ex:append(7, B),	
	{ok,[{6,B},{7,B}]} = file_txn_log_ex:load_logs(5),
	
	ok = file_txn_log_ex:stop(test),
	os:cmd("rm -rf "++Dir),
	ok.
test_trunc_last_zxid(_) ->
	{ok,Root} = file:get_cwd(),
	Dir = Root ++ "/txn_log_test/",
	application:set_env(zab, txn_log_dir, Dir),
	{ok,_} = file_txn_log_ex:start_link(),
	ok = file_txn_log_ex:truncate_last_zxid(),
	{ok,?UNDEFINED_ZXID} = file_txn_log_ex:get_last_zxid(),
	
	file_txn_log_ex:append(17, <<"hello">>),
	{ok,17} = file_txn_log_ex:get_last_zxid(),
	ok = file_txn_log_ex:truncate_last_zxid(),
	{ok,?UNDEFINED_ZXID} = file_txn_log_ex:get_last_zxid(),
	
	
	file_txn_log_ex:append(17, <<0:(512*8)>>),
	file_txn_log_ex:append(18, <<0:(512*8)>>),
	{ok,18} = file_txn_log_ex:get_last_zxid(),
	ok = file_txn_log_ex:truncate_last_zxid(),
	{ok,17} = file_txn_log_ex:get_last_zxid(),
	ok = file_txn_log_ex:truncate_last_zxid(),
	{ok,?UNDEFINED_ZXID} = file_txn_log_ex:get_last_zxid(),
	
	
	ok = file_txn_log_ex:stop(test),
	os:cmd("rm -rf "++Dir),
	ok.
test_all(_) ->
	{ok,Root} = file:get_cwd(),
	Dir = Root ++ "/txn_log_ex_test/",
	application:set_env(zab, txn_log_dir, Dir),
	{ok,_} = file_txn_log_ex:start_link(),
	ok = file_txn_log_ex:stop(test),
	%%--------------------------------------------------------------
	{ok,_} = file_txn_log_ex:start_link(),
	{ok,{0,undefined}} = file_txn_log_ex:get_last_msg(),
	{error,ignored} = file_txn_log_ex:append(0, <<1:32>>),
	ok = file_txn_log_ex:append(1, <<1:32>>),
	{ok,1} = file_txn_log_ex:get_last_zxid(),
	{ok,{1,<<1:32>>}} = file_txn_log_ex:get_last_msg(),
	{error,ignored} = file_txn_log_ex:append(0, <<0:32>>),
	ok = file_txn_log_ex:stop(test),
	%%---test rolllog when data overflow------
	{ok,_} = file_txn_log_ex:start_link(),
	{ok,1} = file_txn_log_ex:get_last_zxid(),
	{ok,{1,<<1:32>>}} = file_txn_log_ex:get_last_msg(),
	ok = file_txn_log_ex:append(2, <<1:(512*8)>>),
	ok = file_txn_log_ex:append(4, <<1:(512*8)>>),
	ok = file_txn_log_ex:stop(test),
	F4 = filename:join([Dir,"log.4"]),
	{ok,#file_info{size = SF4}} = prim_file:read_file_info(F4),
	{ok,_} = file_txn_log_ex:start_link(),
	{ok,4} = file_txn_log_ex:get_last_zxid(),
	{ok,{4,<<1:(512*8)>>}} = file_txn_log_ex:get_last_msg(),
	ok = file_txn_log_ex:stop(test),
	%%---test delete invalide file ------
	{ok,FD0} = prim_file:open(filename:join([Dir,"log.5"]),[write,read,raw,binary]),
	prim_file:close(FD0),
	{ok,_} = file_txn_log_ex:start_link(),
	{ok,4} = file_txn_log_ex:get_last_zxid(),
	ok = file_txn_log_ex:stop(test),
	false = filelib:is_file(filename:join([Dir,"log.5"])),
	os:cmd("rm -rf "++Dir),
	ok.



test_loop_append(_) ->
	{ok,Root} = file:get_cwd(),
	Dir = Root ++ "/txn_log_test/",
	application:set_env(zab, txn_log_dir, Dir),
	{ok,_} = file_txn_log_ex:start_link(),
	BlockList = [8,64,256,1024],
	Times = 10000,
	ok = loop_test_append_block(BlockList,1,Times),
	
	ok = file_txn_log_ex:stop(test),
	ok.
loop_test_append_block([],_,_) -> ok;
loop_test_append_block([Size|T],StartTime,Times) ->
	ST = zab_util:tstamp(),
	ok = append_times(Size,StartTime,Times),
	ET = zab_util:tstamp(),
	?INFO_F("total append ~p record ~pb cost time:~p~n",[Times,Size,(ET-ST)]),
	loop_test_append_block(T, StartTime+Times, Times).


append_times(Size,ST,0) -> ok;
append_times(Size,ST,Times) ->
	ok = file_txn_log_ex:append(ST, <<0:(8*Size)>>),
	append_times(Size,ST+1,Times-1).


