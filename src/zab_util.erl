%% Author: sunshine
%% Created: 2012-7-24
%% Description: TODO: Add description to zab_util
-module(zab_util).

%%
%% Include files
%%

-include("txnlog.hrl").
-include("zab.hrl").
-include("log.hrl").

%%
%% Exported Functions
%%
-export([log_index_to_binary/1,
		 binary_to_log_index/1,
		 log_data_to_binary/1,
		 binary_to_log_data/1,
		 tstamp/0,split_zxid/1,
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

%% data and index to binary.
log_data_to_binary(#log_data{crc32=Crc,length=Length,time=T,value=V}) ->
	{ok,<<Crc:64/integer,Length:64/integer,T:64/integer,V/binary>>};
log_data_to_binary(_) ->
	{error,bad_args}.

binary_to_log_data(<<Crc:64/integer,Length:64/integer,T:64/integer,V/binary>>) ->
	{ok,#log_data{crc32=Crc,length=Length,time=T,value=V}};
binary_to_log_data(_) ->
	{error,bad_args}.


log_index_to_binary(#log_index{offset=Offset,zxid=Zxid,length=Len}) ->
	{ok,<<Zxid:64/integer,Offset:64/integer,Len:64/integer,0:64/integer>>};
log_index_to_binary(_) ->
	{error,bad_args}.

binary_to_log_index(<<Zxid:64/integer,Offset:64/integer,Len:64/integer,0:64/integer>>) ->
	{ok,#log_index{offset=Offset,zxid=Zxid,length=Len}};
binary_to_log_index(_) ->
	{error,bad_args}.


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


notify_caller(Caller,Msg) ->
	Caller ! Msg.

		
%%
%% Local Functions
%%

