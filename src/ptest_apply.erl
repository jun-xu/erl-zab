%% Author: zj
%% Created: jan 9, 2014
%% Description: TODO: Add description to arithmetic
%% just for test only !!!
-module(ptest_apply).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("log.hrl").
-include("zab.hrl").

%% --------------------------------------------------------------------
%% External exports

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,
		 compare/0]).

-record(state, {total=0,count=0,from =undefined,timer}).

%% ====================================================================
%% External functions
%% ====================================================================

%%
%% Exported Functions
%%
-export([call/1,stop/0,result/0,
		 start_link/0
%% 		 get_state/0
]).

%%
%% API Functions
%%

start_link() ->
	gen_server:start_link({local,?MODULE}, ?MODULE, [],[]).

%% get_state() ->
%% 	gen_server:call(?MODULE, get_state).

call(Msg) ->
	gen_server:call(?MODULE,Msg,20000).

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
init([]) ->
    {ok, #state{}}.


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
handle_call({init,Total},_From,State = #state{count=_C,total=_T}) ->
	{reply,ok,State#state{total=Total,count=0}};

handle_call(plus,_From,State = #state{from=F,timer=Ref,count=C,total=T}) ->
	NewCount = C + 1,
	case T - NewCount of
		0 ->
			case Ref of
				undefined ->
					{reply,ok,State#state{count=NewCount}};
				_ ->
					gen_server:reply(F, {ok,finished}),
					timer:cancel(Ref),
					{reply,ok,State#state{from=undefined,timer=undefined}}
			end;
		_ ->
			{reply,ok,State#state{count=NewCount}}
	end;

handle_call({plus,_},_From,State = #state{from=F,timer=Ref,count=C,total=T}) ->
	NewCount = C + 1,
	case T - NewCount of
		0 ->
			case Ref of
				undefined ->
					{reply,ok,State#state{count=NewCount}};
				_ ->
					gen_server:reply(F, {ok,finished}),
					timer:cancel(Ref),
					{reply,ok,State#state{from=undefined,timer=undefined}}
			end;
		_ ->
			{reply,ok,State#state{count=NewCount}}
	end;
	
	

handle_call(compare, _From, State = #state{total=T,count=C}) ->
	case T > 0 of
		true ->
			case T - C of
				0 ->
					{reply,{ok,finished},State#state{timer=undefined,from=undefined}};
				_ ->
					{ok,Ref} = timer:send_interval(100, compareValue),
					{noreply,State#state{from=_From,timer=Ref}} 
			end
	end;
%%     {reply, ok,State};


handle_call(getValue,_From,State = #state{from=F,timer=Ref,count=C,total=T}) ->
	{reply,{C,T},State};

handle_call(_,_,State) ->
	 {reply, ok, State}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(Msg, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info({compareValue,Pid}, State=#state{timer=Ref,from=F,total=T,count=C}) ->
	case C > 0 of
		true ->
			case T - C of
				0 ->
					gen_server:reply(Pid, {ok,finished}),
					timer:cancel(Ref),
					{noreply,State#state{timer=undefined,from=undefined}};
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

compare() ->
	gen_server:call(?MODULE,compare,20*1000).
			
	
	
	
