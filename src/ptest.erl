%% Author: zj
%% Created: jan 9, 2014
%% Description: TODO: Add description to arithmetic
%% just for test only !!!
-module(ptest).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("log.hrl").
-include("zab.hrl").

%% --------------------------------------------------------------------
%% External exports

%% gen_server callbacks
-export([init/2, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {nodePid=0,total=0,count=0}).

%% ====================================================================
%% External functions
%% ====================================================================

%%
%% Exported Functions
%%
-export([call/1,stop/0,result/0,
		 start_link/2
]).

%%
%% API Functions
%%

start_link(NodePid,Total) ->
	gen_server:start_link({local,?MODULE}, ?MODULE, [NodePid,Total],[]).

%% get_state() ->
%% 	gen_server:call(?MODULE, get_state).

call(Msg) ->
	gen_server:call(?MODULE,Msg).

result() ->
	gen_server:call(?MODULE, result).

stop() ->
	gen_server:call(?MODULE,stop).




%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init(NodePid,Total) ->
	timer:send_interval(100, compareValue),
    {ok, #state{nodePid=NodePid,total=Total,count=0}}.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_call(plus, _From, State=#state{count=Count}) ->
    {reply, ok,State#state{count=Count+1}}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info(compareValue, State=#state{nodePid=N,total=T,count=C}) ->
	case C > 0 of
		true ->
			case T - C of
				0 ->
					N! finished,
					{stop,ok,State};
				_ ->
					{noreply, State}
			end;
		_ ->
			{noreply, State}
	end;
    

handle_info(Info, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(Reason, State) ->
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(OldVsn, State, Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------

