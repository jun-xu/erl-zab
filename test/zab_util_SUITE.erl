%% Author: sunshine
%% Created: 2012-7-24
%% Description: TODO: Add description to zab_util_SUITE
-module(zab_util_SUITE).

%%
%% Include files
%%
-include("log.hrl").
-include("txnlog_ex.hrl").
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
%%	test_gen_zxid
	 ].

test_gen_zxid(_) ->
	
	ok.

test_all(_) ->
	
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
	
	
	
	
	