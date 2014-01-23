%% Author: sunshine
%% Created: 2012-7-24
%% Description: TODO: Add description to zab_util
-module(zab_util).

%%
%% Include files
%%

-include("zab.hrl").
-include("log.hrl").

%%
%% Exported Functions
%%
-export([tstamp/0,split_zxid/1,
		 get_app_env/3,
		 get_zab_nodes/0,
%% 		 get_all_live_nodes/0,
		 contains/2,
		 notify_caller/2]).

%%
%% API Functions
%%


tstamp() ->
    {Mega, Sec, Micro} = now(),
    ((Mega * 1000000) + Sec)*1000 + Micro div 1000.


split_zxid(Zxid) ->
	<<Epoch:16,EZxid:48>> = <<Zxid:64>>,
	{Epoch,EZxid}.
	
-spec get_app_env(Key::atom(),Opts::[tuple()],Default::any()) -> any().
get_app_env(Key, Opts, Default) ->
    case proplists:get_value(Key, Opts) of
        undefined ->
					case application:get_env(?APP_NAME,Key) of
                		{ok, Value} -> Value;
                		undefined ->  Default
            		end;
        Value ->
            Value
    end.

get_zab_nodes() ->
	SelfNode = node(),
	Nodes = get_app_env(known_nodes, [],[]),
	case lists:member(SelfNode, Nodes) of
		true ->
			Nodes;
		false ->
			[SelfNode|Nodes]
	end.

contains(_Value,[]) -> false;
contains(Value,[Value|_T]) -> true;
contains(Value,[_|T]) ->
	contains(Value,T).

notify_caller(Callers,Msg) when is_list(Callers)->
%% 	?DEBUG_F("~p -- reply group caller:~p",[?MODULE,Callers]),
	loop_notify_caller(Callers,Msg);
notify_caller(Caller,Msg) ->
	Caller ! Msg.

loop_notify_caller([],_) -> ok;
loop_notify_caller([Caller|T],Msg) ->
	Caller ! Msg,
	loop_notify_caller(T, Msg).
		
%%
%% Local Functions
%%

