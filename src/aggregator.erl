-module(aggregator).

-behaviour(gen_server).

%% API functions
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, { docid, task=[], documentdata, documentagd, document, documentappend=[], device_id, hour, type }).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(DocumentID,Aggregations) ->
    gen_server:start_link(?MODULE, [DocumentID,Aggregations], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([DocumentID,Aggregations]) ->
	lager:info("Document ~p, Aggr ~p",[DocumentID,Aggregations]),
	gen_server:cast(self(), run_task),
	case mng:find_one(ga_mongo,<<"devicedata">>,did2key(DocumentID)) of
		{DATA} ->
			D=mng:m2proplistr(DATA),
			lager:info("DATA ~p~n~p",[DATA,D]),
			{Dat, D2} = case proplists:split(D,[data]) of
							{[[{data,Da1}]],Da2} -> {Da1,Da2};
							{_,Da2} -> {[],Da2}
						end,
			{Agd, D3} = case proplists:split(D2,[aggregated]) of
							{[[{aggregated,CAgd}]],CD3} ->
								{mng:m2proplistr(CAgd), CD3};
							_Any ->
								lager:info("Agd2 ~p",[_Any]), {[],D2}
						end,
			Dev=proplists:get_value(device,D3),
			Hr=proplists:get_value(hour,D3),
			Type=proplists:get_value(type,D3),
			lager:debug("D3   ~p",[D3]),
			lager:debug("Dadg ~p",[Agd]),
			%lager:info("Data ~p",[Dat]),
			{ok, #state{docid=DocumentID, task=Aggregations, documentagd=Agd, documentdata=Dat, document=D3,
					   device_id=Dev, hour=Hr, type=Type}};
		_ -> {stop, nodocument}
	end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(run_task, State) ->
	case State#state.task of
		[] -> 
			AppD=[ { <<"aggregated.",K/binary>>, V } || {K,V} <- State#state.documentappend ],
			Data={'$set', mng:proplisttom(AppD)},
			BinHr=integer_to_binary(State#state.hour),
			AggrD=[ { <<BinHr/binary,".",K/binary>>, V } || {K,V} <- State#state.documentappend ],
			Res=poolboy:transaction(ga_mongo, 
						fun(Worker) ->
								lager:info("Update ~p ~p ~p",[Worker,did2key(State#state.docid), Data]),
								mongo:update(Worker,<<"devicedata">>,did2key(State#state.docid), Data)
						end),
			Mon=gpstools:floor(State#state.hour/720),
			mng:ins_update(ga_mongo,<<"devicedata">>,{
										type, <<"aggregated">>, 
										device_id, State#state.device_id, 
										ymon, Mon 
									   }, mng:proplisttom(AggrD)),

			lager:info("Task ~p: Append ~p: ~p",[did2key(State#state.docid), AppD, Res ]),
			{MSec,Sec,_}=now(),
			UnixTime=MSec*1000000+Sec,

			case State#state.docid of
				DID when is_binary(DID) ->
					NormalFun=fun(Worker) -> 
									  eredis:q(Worker, [ 
														"setex", 
														"aggregate:done:"++binary_to_list(DID), 
														3600, 
														integer_to_list(UnixTime) ]),
									  eredis:q(Worker, [ 
														"publish", 
														"aggregate:done",
														binary_to_list(DID) ])
							  end,
					poolboy:transaction(ga_redis, NormalFun);
				_ ->
					ok
			end,

%			erlang:send_after(1000,self(),{finish}),
%			{noreply, State};
			gen_server:cast(aggregator_dispatcher, {finished, State#state.docid}),
			{stop, normal, State};
		[CTask|Rest] ->
			Append=case catch apply(CTask,process,[
												   State#state.documentdata,
												   {State#state.document,State#state.documentagd},
												   State#state.documentappend
												  ]) of 
				{ok, AppData} -> 
						   MN=list_to_binary(atom_to_list(CTask)),
						   State#state.documentappend++[ {<<MN/binary,".",K/binary>>,V} || {K,V} <- AppData ];
				_Any -> 
						   lager:error("Something went wrong with taks ~p: ~p",[CTask, _Any]),
						   State#state.documentappend
			%catch 
			%	error:X ->
			%		lager:error("Can't run ~p task: ~p",[CTask, X]),
			%		State#state.documentappend
			end,
			gen_server:cast(self(), run_task),
			{noreply, State#state{task=Rest,documentappend=Append}}
	end;

	
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({finish},State) ->
	gen_server:cast(aggregator_dispatcher, {finished, State#state.docid}),
	{stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

did2key(DID) when is_binary(DID) ->
	{'_id',mng:hex2id(DID)};

did2key(DID) when is_tuple(DID) ->
	DID.

