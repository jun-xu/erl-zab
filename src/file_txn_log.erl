%%% -------------------------------------------------------------------
%%% Author  : sunshine
%%% Description :
%%%
%%% Created : 2012-7-24
%%% -------------------------------------------------------------------
-module(file_txn_log).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("log.hrl").
-include("txnlog.hrl").
-include_lib("kernel/include/file.hrl").
%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------
-export([start_link/0,
		 stop/1,append/2,
		 get_last_zxid/0,
		 truncate_last_zxid/0,load_logs/2,
		 load_logs/1,get_last_msg/0,
		 get_value/1]).

%% --------------------------------------------------------------------
%% gen_server callbacks
%% --------------------------------------------------------------------
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(txn_log, {dir,								%% txnlog root dir.
				  current_file,						%% current txn log file FD.
				  current_file_data_offset = ?SIZE_OF_MAGIC_NUM,
				  current_file_index_offset = ?INDEX_START_OFFSET,
				  last_zxid,
				  last_msg
  }).



%% ====================================================================
%% External functions
%% ====================================================================
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec append(Zxid::integer(),Data::binary()) -> ok | {error,ignored}.
append(Zxid,Data) ->
	gen_server:call(?MODULE,{append,Zxid,Data},infinity).

-spec get_last_zxid() -> {ok,Zxid::integer()}.
get_last_zxid() ->
	gen_server:call(?MODULE,get_last_zxid,infinity).

get_last_msg() ->
	gen_server:call(?MODULE,get_last_msg,infinity).

-spec get_value(Zxid::integer()) ->{ok,Value::binary()}|{error,Reason::term()}.
get_value(Zxid) ->
	gen_server:call(?MODULE, {get_value,Zxid},infinity).

-spec truncate_last_zxid() -> ok.
truncate_last_zxid() ->
	gen_server:call(?MODULE, truncate_last_zxid,infinity).

%% binary data start from StartZxid+1 to EndZxid,
%% if EndZxid undefined,will replace with LastZxid
-spec load_logs(StartZxid::integer()) ->{ok,[{Zxid::integer(),Msg::binary()}]} | {error,Reason::term()}.
load_logs(StartZxid) ->
	gen_server:call(?MODULE, {load_log,StartZxid,all},infinity).

load_logs(StartZxid,LoadCount) ->
	gen_server:call(?MODULE, {load_log,StartZxid,LoadCount},infinity).

stop(Reason) ->
	gen_server:call(?MODULE,{stop,Reason}).
%% ====================================================================
%% Server functions
%% ====================================================================

%%----------------------------------------------------------------------
%% @spec init(Args) -> {ok, State}           |
%%                            {ok, State, Timeout}  |
%%                            ignore                |
%%                            {stop, Reason}
%%
%% @doc
%%      Initializes the server
%% @end
%%----------------------------------------------------------------------
init([]) ->
	Dir = zab_util:get_app_env(txn_log_dir, [], "./"),
	?INFO_F("~p -- start txn log with dir:~p~n",[?MODULE,Dir]),
	ok = filelib:ensure_dir(Dir),
	{FD,DataOffset,IndexOffset,LastZxid,LastMsg} = open_last_file(Dir),
    {ok, #txn_log{dir=Dir,current_file=FD,current_file_data_offset=DataOffset,
				  current_file_index_offset=IndexOffset,last_zxid=LastZxid,last_msg=LastMsg}}.

%%--------------------------------------------------------------------
%% @private
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @doc
%% 		Handling call messages
%% @end
%%--------------------------------------------------------------------

%% normally, should not append data which zxid < lastzxid.
handle_call({append,Zxid,_Data}, _From, #txn_log{last_zxid=LastZxid} = State) when Zxid =< LastZxid ->
	?INFO_F("~p -- already append zxid:~p~n",[?MODULE,Zxid]),
	{reply, {error,ignored}, State};
%% roll log when index overflow.
handle_call({append,Zxid,Data}, _From, #txn_log{dir=Dir,current_file=FD,current_file_index_offset=IndexOffset} = State) 
  				when IndexOffset >= ?ALLOC_SIZE_PER_PAGE ->
	{NewFD,NewDataOffset,NewIndexOffset} = roll_log(Dir,FD,Zxid),
	{ok,DataOffset1,IndexOffset1} = append_data_to_file(NewFD, NewDataOffset, NewIndexOffset, Zxid, Data),
    {reply, ok, State#txn_log{current_file=NewFD,current_file_data_offset=DataOffset1,
							  current_file_index_offset=IndexOffset1,last_zxid=Zxid,last_msg=Data}};

handle_call({append,Zxid,Data}, _From, #txn_log{current_file=FD,dir=Dir,current_file_data_offset=DataOffset,
												current_file_index_offset=IndexOffset} = State) ->
	Length = size(Data) + 24,
	{NewFD,NewDataOffset,NewIndexOffset} = case (DataOffset + Length) > ?INDEX_START_OFFSET of
											   true ->
												    %% roll log when data is overflow.
											   		roll_log(Dir,FD,Zxid);
											   false ->
												   	{FD,DataOffset,IndexOffset}
											end,
	{ok,DataOffset1,IndexOffset1} = append_data_to_file(NewFD, NewDataOffset, NewIndexOffset, Zxid, Data),
    {reply, ok, State#txn_log{current_file=NewFD,current_file_data_offset=DataOffset1,
							  current_file_index_offset=IndexOffset1,last_zxid=Zxid,last_msg=Data}};
handle_call({stop,Reason}, _From,#txn_log{current_file=FD} = State) ->
	?INFO_F("~p -- stop by reason:~p~n",[?MODULE,Reason]),
	prim_file:close(FD),
	{stop, normal, ok, State};

handle_call(get_last_zxid, _From,#txn_log{last_zxid=Zxid} = State) ->
    {reply, {ok,Zxid}, State};

handle_call(get_last_msg,_,#txn_log{last_zxid=Zxid,last_msg=Msg} = State) ->
    {reply, {ok,{Zxid,Msg}}, State};

handle_call({get_value,Zxid}, _From,#txn_log{dir=Dir}=State) ->
	Reply = case read_zxid(Dir,Zxid) of
				{ok,Val} ->
					{ok,Val};
				Error ->
					Error
			end,
    {reply, Reply, State};


handle_call(truncate_last_zxid, _From,#txn_log{current_file=FD,current_file_index_offset=IndexOffset} = State) ->
	{LastZxid,Offset,Msg} = if
		IndexOffset == ?INDEX_START_OFFSET ->
			{?UNDEFINED_ZXID,?INDEX_START_OFFSET,undefined};
		IndexOffset == ?INDEX_START_OFFSET + ?SIZE_OF_INDEX_BINARY ->
			{ok,_} = prim_file:position(FD, ?INDEX_START_OFFSET),
			{?UNDEFINED_ZXID,?INDEX_START_OFFSET,undefined};
		true ->
			NewOffset = IndexOffset-(2*?SIZE_OF_INDEX_BINARY),
			{ok,_} = prim_file:position(FD, NewOffset),
			{ok,B} = prim_file:read(FD, ?SIZE_OF_INDEX_BINARY),
			{ok,#log_index{offset=DataOffset,zxid=Zxid,length=Len}} = zab_util:binary_to_log_index(B),
			{ok,_} = prim_file:position(FD, DataOffset),
			{ok,DataBin} = prim_file:read(FD, Len),
			{ok,#log_data{value=Value}} = zab_util:binary_to_log_data(DataBin),
			{Zxid,NewOffset+?SIZE_OF_INDEX_BINARY,Value}
	end,
	ok = prim_file:truncate(FD),
    {reply, ok, State#txn_log{current_file_index_offset=Offset,last_zxid=LastZxid,last_msg=Msg}};

handle_call({load_log,StartZxid,_LoadCount}, _From,#txn_log{last_zxid=LastZxid} = State) when StartZxid > LastZxid->
	{reply, {error,not_found}, State};
handle_call({load_log,LastZxid,_LoadCount}, _From,#txn_log{last_zxid=LastZxid} = State) ->
	{reply, {ok,[]}, State};
handle_call({load_log,StartZxid,LoadCount}, _From,#txn_log{dir=Dir} = State) ->
    Reply = load_logs0(Dir,StartZxid,LoadCount),
	{reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @doc
%% 		Handling cast messages
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @doc
%% 		Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @spec terminate(Reason, State) -> void()
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
terminate(Reason, #txn_log{current_file=FD}) ->
	case FD of
		undefined -> ok;
		_ -> 
			prim_file:close(FD),
			ok
	end,
	?WARN_F("~p -- terminate by reason:~p~n",[?MODULE,Reason]),
    ok.

%%-------------------------------------------------------------------------
%% @private
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc  Convert process state when code is changed.
%% @end
%%-------------------------------------------------------------------------
code_change(OldVsn, State, Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------

%% filter logs with name which start with "log."
open_last_file(Dir) ->
	Files = filelib:fold_files(Dir, "log.[0-9]+", false,
                               fun(F, Acc) ->
									case get_base_name(F) of
										V ->
											[V|Acc]
									end
                               end, []
					  ),
	open_last_file0(Dir,Files).
	
%% if txn log dir is empty,create new.
open_last_file0(Dir,[]) ->
	{ok,FD,DataOffset,IndexOffset} = create_new_file(Dir,0),
	{FD,DataOffset,IndexOffset,0,undefined};

%% otherwise,open the biggest zxid file.
open_last_file0(Dir,Files) ->
	[{_,LastFile}|T] = lists:reverse(lists:sort(Files)),
	case open_file(LastFile) of
		{error,_} ->
			prim_file:delete(LastFile),
			open_last_file0(Dir,T);
		{ok,V} ->
			V
	end.

create_new_file(Dir,Zxid) ->
	{ok,FD} = prim_file:open(filename:join([Dir,lists:concat(["log.",Zxid])]),?FILE_WRITE_OPEN_OPTIONS),
	prim_file:write(FD,?MAGIC_NUM),
	{ok,FD,?SIZE_OF_MAGIC_NUM,?INDEX_START_OFFSET}.

open_file(FileName) ->
	%% truncate file if file is invalidate.
	ok = truncate(FileName),
	{ok,FD} = prim_file:open(FileName,?FILE_WRITE_OPEN_OPTIONS),
	case prim_file:read(FD, ?SIZE_OF_MAGIC_NUM) of
		{ok,?MAGIC_NUM} ->
			{ok,#file_info{size = Size}} = prim_file:read_file_info(FileName),
			case Size > ?INDEX_START_OFFSET of
				true ->
					%% read the last zxid from file.
					{ok,_} = prim_file:position(FD, Size-?SIZE_OF_INDEX_BINARY),
					{ok,B} = prim_file:read(FD, ?SIZE_OF_INDEX_BINARY),
					{ok,#log_index{offset=Offset,zxid=Zxid,length=Len}} = zab_util:binary_to_log_index(B),
					{ok,_} = prim_file:position(FD, Offset),
					{ok,DataBin} = prim_file:read(FD, Len),
					{ok,#log_data{value=Value}} = zab_util:binary_to_log_data(DataBin),
					{ok,{FD,Offset+Len,Size,Zxid,Value}};
				false ->
					%% maybe delete file when the file is empty.
					?INFO_F("~p -- open txn file error by reason:~p .~n",[?MODULE,empty]),
					{error,empty_file}
			end;
		_ ->
			%% maybe delete file when first 4 byte is not magic number.
			?INFO_F("~p -- error file:~p of magic num.~n",[?MODULE,FileName]),
			prim_file:close(FD),
			{error,invalidate_file}
	end.

get_base_name(FileName) ->
	{match,[ZxidStr]} =  re:run(filename:basename(FileName),"^log.([0-9]+)$",[{capture,all_but_first,list}]),
	{list_to_integer(ZxidStr),FileName}.


truncate(FileName) ->
	{ok,#file_info{size = Size}} = prim_file:read_file_info(FileName),
	
	case Size > ?INDEX_START_OFFSET of
		true ->
			case Size rem 32 of
				0 -> ok;
				BadSize ->
					?WARN_F("~p -- truncate file:~p~nfilesize:~p~n",[?MODULE,FileName,Size]),
					{ok,FD} = prim_file:open(FileName,?FILE_WRITE_OPEN_OPTIONS),
					{ok,_} = prim_file:position(FD, Size-BadSize),
					ok = prim_file:truncate(FD),
					prim_file:close(FD),
					ok
			end;
		false ->
			ok
	end.
	
append_data_to_file(FD,DataOffset,IndexOffset,Zxid,Data) ->
%% 	?INFO_F("append data:~p to offset:~p~n",[Zxid,DataOffset]),
	{ok,_} = prim_file:position(FD, DataOffset),
	Crc = erlang:crc32(Data),
	Length = size(Data),
	{ok,B} = zab_util:log_data_to_binary(#log_data{crc32=Crc,time=zab_util:tstamp(),length=Length,value=Data}),
	ok = prim_file:write(FD,B),
	{ok,_} = prim_file:position(FD, IndexOffset),
	{ok,IB} = zab_util:log_index_to_binary(#log_index{offset=DataOffset,zxid=Zxid,length=size(B)}),
	ok = prim_file:write(FD, IB),
	{ok,DataOffset+size(B),IndexOffset+32}.


%% {NewFD,NewDataOffset,NewIndexOffset}
roll_log(Dir,FD,Zxid) ->
	prim_file:close(FD),
	{ok,NewFD,NewDataOffset,NewIndexOffset} = create_new_file(Dir,Zxid),
	{NewFD,NewDataOffset,NewIndexOffset}.
	
-spec load_one_file(Fullname::string(),Zxid::integer(),L::[{Zxid::integer(),Value::binary()}],
					TotalAccount::integer() | all) -> 
		  {ok,Msgs::[{Zxid::integer(),Value::binary()}],RemainAccount::integer()|all} | {error,Reason::any()}.
load_one_file(Fullname,Zxid,L,TotalAccount) ->
	{ok,FD} = prim_file:open(Fullname,[read,binary]),
	Ext = filename:extension(Fullname),
	Num = list_to_integer(tl(Ext)),
	R = if
		Num > Zxid orelse 0 == Zxid->
			{ok,?INDEX_START_OFFSET-?SIZE_OF_INDEX_BINARY};
		true ->
			{ok,_} = prim_file:position(FD, ?INDEX_START_OFFSET),
			case prim_file:read(FD,?INDEX_ALLOC_SIZE_PER_PAGE) of
				{ok,Bin} ->
					search_start_offset(Bin,Zxid);
				eof ->
					eof
			end
	end,
	R1 = case R of
		{ok,Offset} ->
			{ok,_} = prim_file:position(FD, Offset+?SIZE_OF_INDEX_BINARY),
			case prim_file:read(FD,?INDEX_ALLOC_SIZE_PER_PAGE) of
				{ok,Bin1} ->				
					loop_load_log(FD,Bin1,lists:reverse(L),TotalAccount);
				eof ->
					{ok,L,TotalAccount}
			end;
		eof ->
			{ok,L,TotalAccount};
		Error ->
			Error
	end,
	prim_file:close(FD),
	R1.
				
load_logs0(Dir,Zxid,TotalAccount) ->
	Fun = fun(Fullname,AccIn) ->
				  Ext = filename:extension(Fullname),
				  Num = list_to_integer(tl(Ext)),
				  if
					  Num > Zxid ->
						  [Fullname|AccIn];
					  true ->
						  AccIn
				  end
		  end,
	HighFiles = filelib:fold_files(Dir, "log\.[0-9]+$", false, Fun, []),
%% 	?DEBUG_F("~p -- load logs:~p amount:~p files:~p~n",[?MODULE,Zxid,TotalAccount,HighFiles]),
	case scanner_near_file(Dir,Zxid) of
		{ok,File} ->
%% 			?DEBUG_F("~p -- scanner new file:~p of ~p result:~p~n",[?MODULE,File,Zxid,load_one_file(File,Zxid,[],TotalAccount)]),
			case load_one_file(File,Zxid,[],TotalAccount) of
				{ok,L,0} -> {ok,L};
				{ok,L1,RemainAccount} ->
%% 					?DEBUG_F("~p -- loop load msgs from ~p.~n",[?MODULE,{lists:sort(HighFiles),{L1,RemainAccount}}]),
					loop_load_msgs_of_log(Zxid, lists:sort(HighFiles), L1, RemainAccount);
				Error ->
					Error
			end;
		Error ->
			Error
	end.


loop_load_msgs_of_log(_,_,{error,_}=E,_) -> E;
loop_load_msgs_of_log(_,[],L,_) -> {ok,L};
loop_load_msgs_of_log(_,_,L,0) -> {ok,L};
loop_load_msgs_of_log(Zxid,[File|T],L,Account) ->
%% 	?DEBUG_F("~p -- load msg from:~p of ~p with account:~p~n",[?MODULE,Zxid,File,Account]),
	case load_one_file(File,Zxid,L,Account) of
		{ok,L1,NewAccount} ->
			loop_load_msgs_of_log(Zxid,T,L1,NewAccount);
		E -> E
	end.


loop_load_log(_FD,<<>>,L,Account) -> {ok,lists:reverse(L),Account};
loop_load_log(_,_,L,0) -> {ok,lists:reverse(L),0};
loop_load_log(FD,<<Bin:32/binary,Rest/binary>>,L,Account) ->
	{ok,#log_index{zxid=Zxid,offset=DataOffset,length=Len}} = zab_util:binary_to_log_index(Bin),
	{ok,_} = prim_file:position(FD, DataOffset),
	{ok,DataBin} = prim_file:read(FD, Len),
	{ok,#log_data{crc32=CRC,value=Value}} = zab_util:binary_to_log_data(DataBin),
%% 	?DEBUG_F("~p -- load msg0:~p~n",[?MODULE,{Zxid,Value}]),
	case erlang:crc32(Value) of
		CRC ->
			case Account of
				all ->
%% 					?DEBUG_F("~p -- load msg:~p~n",[?MODULE,{Zxid,Value}]),
					loop_load_log(FD,Rest,[{Zxid,Value}|L],all);
				_ ->
					loop_load_log(FD,Rest,[{Zxid,Value}|L],Account - 1)
			end;
			
		true ->
			{error,invalid_log_data}
	end.
			

search_start_offset(Bin,Zxid) ->
	search_start_offset(Bin,Zxid,0).
search_start_offset(<<>>,_,_) ->
	{error,not_found};
search_start_offset(<<Bin:32/binary,Rest/binary>>,Zxid,Seqno) ->
	{ok,#log_index{zxid=Zxid1}} = zab_util:binary_to_log_index(Bin),
	if
		Zxid == Zxid1 ->
			{ok,?INDEX_START_OFFSET+Seqno*?SIZE_OF_INDEX_BINARY};
		true ->
			search_start_offset(Rest,Zxid,Seqno+1)
	end.

read_value_by_zxid(FD,Zxid) ->
	{ok,_} = prim_file:position(FD, ?INDEX_START_OFFSET),
	case prim_file:read(FD, ?INDEX_ALLOC_SIZE_PER_PAGE) of
		{ok,Bin} ->
			case search_start_offset(Bin,Zxid) of
				{ok,Offset} ->
					{ok,_} = prim_file:position(FD, Offset),
					{ok,Bin1} = prim_file:read(FD, ?SIZE_OF_INDEX_BINARY),
					{ok,#log_index{offset=Offset1,length=Len}} = zab_util:binary_to_log_index(Bin1),
					{ok,_} = prim_file:position(FD, Offset1),
					{ok,DataBin} = prim_file:read(FD, Len),
					{ok,#log_data{value=Value}} = zab_util:binary_to_log_data(DataBin),
					{ok,Value};
				Error ->
					Error
			end;
		 eof ->
			 {error,not_found}
	end.

scanner_near_file(Dir,0) ->
	{ok,filename:join(Dir, "log.0")};

scanner_near_file(Dir,Zxid) ->
	Fun = fun(FullName,AccIn) ->
				  FileName = filename:extension(FullName),
				  Zxid1 = list_to_integer(tl(FileName)),
				  [Zxid1|AccIn]
		  end,
	L1 = filelib:fold_files(Dir, "log\.[0-9]+$", false, Fun, []),
	L2 = lists:foldl(fun(X,AccIn) when X =< Zxid->
						[X|AccIn];
				   (_X,AccIn) ->
						AccIn
				end, [], L1),
	case L2 of
		[] ->
			{error,not_found};
		_ ->
			N = hd(lists:reverse(lists:sort(L2))),
			{ok,filename:join(Dir, "log." ++ integer_to_list(N))}
	end.
	
	
read_zxid(Dir,Zxid) ->
	case scanner_near_file(Dir,Zxid) of
		{ok,File} ->
			{ok,FD} = prim_file:open(File, [read,binary]),
			Result = read_value_by_zxid(FD,Zxid),
			prim_file:close(FD),
			Result;
		Error ->
			Error
	end.			

	