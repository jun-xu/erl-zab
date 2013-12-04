%% Author: sunshine
%% Created: 2012-8-22
%% Description: TODO: Add description to election
-module(election).

%%
%% Include files
%%
-include("log.hrl").
%%
%% Exported Functions
%%
-export([]).

%%
%% API Functions
%%

-export([behaviour_info/1]).
-export([start_link/0,start_election/2,handle_vote/1]).

behaviour_info(callbacks) ->
    [{start_link,0},
	 {start_election,2},
	 {stop,0},
	 {handle_vote,1}];
behaviour_info(_) ->
    undefined.

%%
%% Local Functions
%%

start_link() -> {error,not_implement}.
start_election(_ZabNodes,_Quorum) -> {error,not_implement}.
handle_vote(_Msg) -> 
	{error,not_implement}.