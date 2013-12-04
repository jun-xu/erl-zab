%% Author: sunshine
%% Created: 2012-7-24
%% Description: TODO: Add description to zab_util_SUITE
-module(zab_util_SUITE).

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
	test_all
	 ].


test_all(_) ->
	{ok,B} = {ok,<<Zxid:64,Offset:64,3:64,0:64>>} = zab_util:log_index_to_binary(#log_index{zxid=1,offset=2,length=3}),
	{error,bad_args} = zab_util:log_index_to_binary(123),
	{ok,#log_index{zxid=1,offset=2,length=3}} = zab_util:binary_to_log_index(B),
	{error,bad_args} = zab_util:binary_to_log_index(123),
	V = <<1:8>>,
	{ok,B1} = {ok,<<2:64,1:64,3:64,V/binary>>} = zab_util:log_data_to_binary(#log_data{length=1,time=3,crc32=2,value=V}),
	{error,bad_args} = zab_util:log_data_to_binary(123),
	{ok,#log_data{length=1,crc32=2,time=3,value=V}} = zab_util:binary_to_log_data(B1),
	{error,bad_args} = zab_util:binary_to_log_data(123),
	
	T = zab_util:tstamp(),
	
	L = ['zab0','zab1','zab2'],
	false = zab_util:contains(123, L),
	true = zab_util:contains('zab0', L),
	true = zab_util:contains('zab2', L),
	SelfNode = node(),
	[SelfNode] = zab_util:get_zab_nodes(),
	application:set_env(?APP_NAME, known_nodes, [SelfNode]),
	[SelfNode] = zab_util:get_zab_nodes(),
	zab_util:notify_caller(self(), test),
	receive
		test -> ok
		after 1000 ->
			exit(test_failed)
	end,
	
	{0,1} = zab_util:split_zxid(1),
	
	123 = zab_util:get_app_env(test, [{test,123}], 1),
	
	ok.
	
	
	
	
	