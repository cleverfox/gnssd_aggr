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

-record(state, { 
		  docid, 
		  task=[], 
		  documentdata, 
		  documentagd, 
		  document, 
		  documentappend=[], 
		  device_id, 
		  replace=[],
		  hour, 
		  type,
		  fetchfun,
		  hints,
		  config
		 }).

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
init([DocumentID,Tasks]) ->
	lager:debug("Document ~p, Aggr ~p",[DocumentID,Tasks]),
	gen_server:cast(self(), run_task),
	case mng:find_one(ga_mongo,<<"devicedata">>,did2key(DocumentID)) of
		{DATA} ->
			D=mng:m2proplistr(DATA),
			%lager:info("DATA ~p~n~p",[DATA,D]),
			{Dat, D2} = case proplists:split(D,[data]) of
							{[[{data,Da1}]],Da2} -> {Da1,Da2};
							{_,Da2} -> {[],Da2}
						end,
			{Agd, D3} = case proplists:split(D2,[aggregated]) of
							{[[{aggregated,CAgd}]],CD3} ->
								{mng:m2proplistr(CAgd), CD3};
							_Any ->
								%lager:info("Agd2 ~p",[_Any]), 
								{[],D2}
						end,
			Dev=proplists:get_value(device,D3),
			Hr=proplists:get_value(hour,D3),
			Type=proplists:get_value(type,D3),
			lager:debug("D3   ~p",[D3]),
			lager:debug("Dadg ~p",[Agd]),

			Hints=try
					  case mng:find_one(ga_mongo,<<"hints">>,{device,Dev,hour,Hr,type,hints}) of
						  {HINTS} -> 
							  proplists:get_value(hints, mng:m2proplistr(HINTS), []);
						  _ -> []
					  end
				  catch _:_ ->
							[]
				  end,

			%lager:info("Data ~p",[Dat]),
			%
			{DevCfg,AgCfg,AgReplace}=try
					   RedFun=fun(Worker) -> 
							   eredis:q(Worker, [ 
												 "get", 
												 "device:config:"++integer_to_list(Dev)
												])
					   end,
					   {ok,BinCfg}=poolboy:transaction(ga_redis, RedFun),
					   WholeCfg=binary_to_term(BinCfg),
					   {
						WholeCfg, 
						case proplists:get_value(aggregator_config,WholeCfg) of
							undefined -> 
								[];
							AGC when is_list(AGC) ->
								lists:map(fun
											  ({K,V}) when is_atom(K) -> 
												  {K,V};
											  ({K,V}) when is_list(K) -> 
												  Name=try 
														   list_to_existing_atom(K)
													   catch 
														   _:_ -> K
													   end,
												  {Name, V};
											  (V) -> 
												  {unknown, V}
										  end, AGC)
						end,
						case proplists:get_value(aggregators_alias,WholeCfg) of
							undefined -> 
								[];
							AGC when is_list(AGC) ->
								lists:map(fun
											  ({K,V}) -> 
												  {to_eatom(K),to_eatom(V)};
											  (V) -> 
												  {unknown, V}
										  end, AGC)
						end
					   }
					   
				   catch ErC:ErR ->
							 lager:notice("DevCfg error ~p:~p",[ErC,ErR]),
							 {[],[],[]}
				   end,
%			lager:info("Cfg ~p",[AgCfg]),
%			lager:info("AgReplace ~p",[AgReplace]),
			Aggregations1=case Tasks of
							  default -> 
								 %lager:debug("Agg ~p",[DevCfg]),
								  case proplists:get_value(aggregators_autorun,DevCfg) of
									  Aggs0 when is_list(Aggs0) ->
										  lists:filtermap(fun
															  (E) when is_atom(E) ->
																  {true, E};
																	(E) when is_list(E) ->
																  try 
																	  {true, list_to_existing_atom(E)}
																  catch _:_ -> false
																  end;
																	(_) -> false
														  end,Aggs0);
									  _ ->
										  [agg_distance,agg_fuelmeter,agg_fuelgauge3]
								  end;
							  _ ->
								  Tasks 
						  end,
			Aggregations=[agg_distance]++(Aggregations1--[agg_distance]),
			{ok, #state{
					config=#{
					  device=>DevCfg,
					  aggregators=>maps:from_list(AgCfg)
					 },
					replace=AgReplace,
					docid=DocumentID, 
					task=Aggregations,
				   	documentagd=Agd, 
					documentdata=Dat, 
					document=D3,
					device_id=Dev, 
					hour=Hr, 
					type=Type,
					hints=Hints,
					fetchfun=fun(FHour) ->
									 Hour=case FHour of
											  _ when is_integer(FHour) -> 
												  FHour;
											  prev -> 
												  Hr-1;
											  next ->
												  Hr+1;
											  _ ->
												  throw({"Bad hour",FHour})
										  end,
									 case mng:find_one(ga_mongo,<<"devicedata">>,{device,Dev,hour,Hour}) of
										{DATAx} ->
											 mng:m2proplistr(DATAx);
											_ -> nodocument
									 end
							 end
				   }
			};
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
handle_cast(run_task, State) when State#state.task==[] ->
			AppD=[ { <<"aggregated.",K/binary>>, V } || {K,V} <- State#state.documentappend ],
			Data={'$set', mng:proplisttom(AppD)},
			lager:info("Device ~p Hour ~p",[State#state.device_id,State#state.hour]),
			BinHr=integer_to_binary(State#state.hour),
			AggrD=[ { <<BinHr/binary,".",K/binary>>, V } || {K,V} <- State#state.documentappend ],
			Res=poolboy:transaction(ga_mongo, 
						fun(Worker) ->
								lager:debug("Update ~p ~p ~p",[Worker,did2key(State#state.docid), Data]),
								mongo:update(Worker,<<"devicedata">>,did2key(State#state.docid), Data)
						end),
			Mon=gpstools:floor(State#state.hour/720),
			mng:ins_update(ga_mongo,<<"devicedata">>,{
										type, <<"aggregated">>, 
										device, State#state.device_id, 
										umon, Mon 
									   }, mng:proplisttom(AggrD)),

			lager:debug("Task ~p: Append ~p: ~p",[did2key(State#state.docid), AppD, Res ]),
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
%			gen_server:cast(aggregator_dispatcher, {finished, State#state.docid}),
			{stop, normal, State};

handle_cast(run_task, State) ->
	[CrTask|Rest] = State#state.task,
	CTask=proplists:get_value(CrTask, State#state.replace, CrTask),
	lager:debug("Car ~p Run ~p(~p)",[State#state.device_id, CTask,CrTask]),
	Append=case catch apply(CTask,process,[
										   State#state.documentdata,
										   {
											State#state.document ++ [
											 {fetchfun, State#state.fetchfun},
											 {hints, State#state.hints}
											]
											,
											State#state.documentagd
										   },
										   State#state.documentappend,
										   State#state.config
										  ]) of 
			   {ok, AppData} -> 
				   TN=try 
						  apply(CTask,storename,[])
					  catch 
						  _:_ -> 
							  CTask
					  end,
				   MN=list_to_binary(atom_to_list(TN)),
				   State#state.documentappend++[ {<<MN/binary,".",K/binary>>,V} || {K,V} <- AppData ];
			   _Any -> 
				   lager:error("Something went wrong with task ~p: ~p",[CTask, _Any]),
				   lists:map(fun(E)->
									 lager:error("At ~p",[E])
							 end,erlang:get_stacktrace()),

				   State#state.documentappend
				   %catch 
				   %	error:X ->
				   %		lager:error("Can't run ~p task: ~p",[CTask, X]),
				   %		State#state.documentappend
		   end,
	gen_server:cast(self(), run_task),
	{noreply, State#state{task=Rest,documentappend=Append}};

	
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
%	gen_server:cast(aggregator_dispatcher, {finished, State#state.docid}),
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

to_eatom(L) when is_atom(L) ->
	L;
	
to_eatom(L) when is_binary(L) ->
	to_eatom(binary_to_list(L));

to_eatom(L) when is_list(L) ->
	try 
		list_to_existing_atom(L)
	catch 
		_:_ -> L
	end.

