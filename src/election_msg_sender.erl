%% Author: sunshine
%% Created: 2012-8-23
%% Description: TODO: Add description to election_msg_sender
-module(election_msg_sender).

%%
%% Include files
%%
-include("log.hrl").

%%
%% Exported Functions
%%
-export([notify_all_followers_to_commit/1,send_vote_to_nodes/2,send_ack_to_leader/2,send_vote_ack_to_node/2,public_leader_to_nodes/2]).

%%
%% API Functions
%%
notify_all_followers_to_commit(Followers) ->
	rpc:abcast(Followers, zab_manager, {leader_commit,node()}).


send_vote_to_nodes(ZabNodes,Vote) ->
	rpc:abcast(lists:delete(node(), ZabNodes), zab_manager, {election,Vote}).

send_vote_ack_to_node(Node,NewVote) ->
	rpc:abcast([Node], zab_manager, {election_ack,NewVote}).	


public_leader_to_nodes(Followers,Notification) ->
	?DEBUG_F("~p -- ~p send leader notify:~p to nodes:~p~n",[?MODULE,node(),Notification,lists:delete(node(), Followers)]),
	rpc:abcast(lists:delete(node(), Followers), zab_manager, {leader,Notification}).

send_ack_to_leader(Leader,Notification) ->
	rpc:abcast([Leader], zab_manager, {leader_ack,Notification}).

%%
%% Local Functions
%%

