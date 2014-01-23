%% Author: clues
%% Created: Apr 1, 2013
%% Description: TODO: Add description to zab
-module(zab).

%%
%% Include files
%%
-include("zab.hrl").
-include("log.hrl").

%%
%% Exported Functions
%%
-export([write/1,
		 get_state/0,
		 get_last_zxid/0,
		 get_value/1,
		 get_last_commit_zxid/0,
		 split_zxid/1,
		 load_msgs_from/1,
		 test_plus/2]).

%%
%% API Functions
%%

-spec write(Msg::term()) -> {ok,Zxid::integer()} | {error,Reason::term()}.
write(Msg)->
	{ok,{ServerState,_,Leader}} = zab_manager:get_state(),
	case ServerState of
		?SERVER_STATE_LOOKING ->
			{error,server_unavailable};
		?SERVER_STATE_FOLLOWING ->
			rpc:cast(Leader, zab_sync_leader, sync, [term_to_binary(Msg),self()]),
			waitting_ack();
		?SERVER_STATE_LEADING ->
			zab_sync_leader:sync(term_to_binary(Msg),self()),
			waitting_ack()
	end.

get_last_zxid() ->
	file_txn_log_ex:get_last_zxid().

get_state() ->
	zab_manager:get_state().

get_value(Zxid) ->
	case file_txn_log_ex:get_value(Zxid) of
		{ok,Val} ->
			{ok,binary_to_term(Val)};
		Error ->
			Error
	end.

load_msgs_from(Zxid) ->
	file_txn_log_ex:load_logs(Zxid).

get_last_commit_zxid() ->
	zab_apply_server:get_last_commit_zxid().

-spec split_zxid(Zxid::integer()) -> {Epoch::integer(),Counter::integer()}.
split_zxid(Zxid) ->
	zab_util:split_zxid(Zxid).

waitting_ack() ->
	receive
		Any ->
			Any
	after ?TIME_OUT_WRITE ->
			?DEBUG_F("~p -- send msg timout",[?MODULE]),
			{error,timeout}
	end.

-spec test_plus(N::integer(),Max::integer()) -> ok.
test_plus(N,Max) ->
	St = zab_util:tstamp(),
	ok = lists:foreach(fun(_) ->zab:write({plus,N}) end,lists:seq(1, Max)),
	En = zab_util:tstamp(),
	CostTime = En - St,
	error_logger:info_msg("~p ====test use time:~p(ms)~n",[?MODULE,CostTime]),
	ok.