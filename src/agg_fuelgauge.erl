-module(agg_fuelgauge).

-export([process/4,findFirst/2]).

-record(pi_fuel_pdi, {
		  dt,
		  value,
		  prevsd=0,
		  prevsu=0,
		  prevc=0,
		  postsd=0,
		  postsu=0,
		  postc=0,
		  event
		 }).

%recalc
%f(C),C=mng:find(mongo,<<"devicedata">>,{device,864,hour,{'$gt',trunc(1433140800/3600)}},{'_id',1}),f(Res),Res=mc_cursor:rest(C),mc_cursor:close(C)
%[ poolboy:transaction(redis,fun(W)-> eredis:q(W,[ "lpush", <<"aggregate:express">>, <<(list_to_binary(mng:id2hex(ID)))/binary,":agg_fuelgauge">>]) end) || {'_id',ID} <-Res ].

findFirst([],_) ->
		throw('no_sufficient_data');
findFirst([L|Rest],Time) ->
	case L#pi_fuel_pdi.dt>=Time of
		true -> 
			L;
		_ -> 
			findFirst(Rest,Time)
	end.

process(_Data,{_ExtInfo,_PrevAggregated},_Prev,_Config) ->
	{ok, [ ]}.

process_old(Data,{ExtInfo,_PrevAggregated},_Prev,Config) ->
	{MSec,Sec,_} = now(),
	Unixtime=MSec*1000000 + Sec,
	CHour=proplists:get_value(hour,ExtInfo),
	DevID=proplists:get_value(device,ExtInfo),
	lager:error("Stated OLD agg_fuelgauge for ~p:~p",[DevID,CHour]),
	AllC=maps:get(aggregators,Config,#{}),
	MyC=maps:get(?MODULE,AllC,[]),
	lager:debug("Car ~p My Config ~p",[DevID,MyC]),
	MinRF=proplists:get_value(minrefuel,MyC,
							  proplists:get_value("minrefuel",MyC,
												  0.1
												 )
							 ),
	MinDump=proplists:get_value(mindump,MyC,
								proplists:get_value("mindump",MyC,
													0.1
												   )
							   ),
	Averages=proplists:get_value(averages,MyC,
								 proplists:get_value("averages",MyC,
													 120
													)
								),
	Threshold=proplists:get_value(threshold,MyC,
								  proplists:get_value("threshold",MyC,
													  160
													 ) 
								 ),
	EvThr={
	  proplists:get_value(threshold_h,MyC,
						  proplists:get_value("threshold_h",MyC,
											  10
											 ) 
						 ),
	  proplists:get_value(threshold_l,MyC,
						  proplists:get_value("threshold_l",MyC,
											  2
											 ) 
						 )
	 },
	PH=try case proplists:get_value(fetchfun,ExtInfo) of
			   Fu when is_function(Fu) ->
				   BegTime=(CHour*3600)-(Averages*2),
				   DATA=Fu(prev),
				   DevData=proplists:get_value(data,DATA),
				   lists:filter(fun(L) ->
										proplists:get_value(dt,L)>=BegTime
								end, DevData)
		   end
	   catch _:_ ->
				 []
	   end,
	%lager:debug("Car ~p PrevHr prepend ~p",[DevID,PH]),

	HistProc=fun(DS) ->
					 case proplists:get_value(dt,DS) of
						 undefined ->
							 false;
						 DT ->
							 case proplists:get_value(v_fuel,DS) of
								 undefined ->
									 false;
								 0 -> 
									 false;
								 FV when is_float(FV) orelse is_integer(FV)->
									 {true,
									  #pi_fuel_pdi{ dt=DT, value=FV }
									 };
								 _ -> false
							 end
					 end
			 end,
	IHist1=lists:filtermap(HistProc, PH),
	IHist2=lists:filtermap(HistProc, Data),
	IHist=IHist1++IHist2,
	try case length(IHist) > 2 of
			true ->
				%IH1=[ #pi_fuel_pdi{ dt=T, value=L } || {T,L} <- IHist ],
				T1=traverse(IHist,[],[],Averages,a),
				T2=traverse(T1,[],[],Averages,b),
				First=findFirst(T2,CHour*3600),

				T2L=lists:map(fun(E) -> tl( erlang:tuple_to_list(E)) end, T2),
				%lager:info("T2: ~p",[ T2L ]),
				file:write_file(<<"/tmp/agg_fuelgauge.dump.",
								  (integer_to_binary(DevID))/binary,
								  ".",
								  (integer_to_binary(CHour))/binary,
								  ".json">>, 
								iolist_to_binary(mochijson2:encode( T2L ))),


				[Last|_]=T1,
				TotalDiff=
				if Last#pi_fuel_pdi.prevc > 1 ->
					   Last#pi_fuel_pdi.prevsu/Last#pi_fuel_pdi.prevc;
				   true ->
					   Last#pi_fuel_pdi.value 
				end
				- 
				if First#pi_fuel_pdi.prevc > 1 ->
					   First#pi_fuel_pdi.prevsu/First#pi_fuel_pdi.prevc;
				   true ->
					   First#pi_fuel_pdi.value 
				end,
				Events=findev(T2,Averages,0,Threshold,MinRF,MinDump),
				lager:debug("Car ~p Found fuel events ~p",[DevID,Events]),

				Changes=lists:foldl(fun({_,Amount},Sum) ->

											Sum+Amount
									end, 0, Events),

				poolboy:transaction(ga_redis,
									fun(W)->
											DKey="device:fuel:"++integer_to_list(DevID)++":"++integer_to_list(CHour)++":",
											if 
												length(Events) > 0 ->
													KeyH=DKey++"events",
													RLS=lists:foldl(
														  fun({TS,Amount},Sum) ->
																  Sum++[TS,
																		float_to_binary(Amount, [{decimals, 4}, compact])
																	   ]
														  end, 
														  ["hmset",KeyH], 
														  Events),
													eredis:q(W, RLS),
													eredis:q(W, ["expire", KeyH, 7200] );
												true -> 
													noevents
											end,
											eredis:q(W, ["setex", DKey++"lastrun", 7200, Unixtime] ) 
									end),


				lager:debug("Car ~p Delta ~p / ~p / ~p",[DevID, TotalDiff, Changes, TotalDiff - Changes]),

				GetTF=fun(#pi_fuel_pdi{ dt=CDT, value=CFV }) ->
							  {CDT, CFV}
					  end,
				{Time1,Fuel1}=GetTF(hd(IHist2)),
				{Time2,Fuel2}=GetTF(lists:last(IHist2)),
				lager:info("Time ~p -> ~p, Fuel ~p -> ~p",
						   [Time1,Time2,Fuel1,Fuel2]),
				{ok, [
					  {<<"v">>,1},
					  {<<"sum">>,TotalDiff - Changes},
					  {<<"averages">>,Averages},
					  {<<"minrefuel">>,MinRF},
					  {<<"mindump">>,MinDump},
					  {<<"threshold">>,Threshold},
					  {<<"diff">>,TotalDiff},
					  {<<"events">>,[ {dt,EvT,amount,EvA} || {EvT,EvA} <-Events]},
					  {<<"t">>, Unixtime},
					  {<<"dt1">>,Time1},
					  {<<"dt2">>,Time2},
					  {<<"fuel1">>,Fuel1},
					  {<<"fuel2">>,Fuel2},
					  {<<"p_total">>,length(Data)},
					  {<<"p_valid">>,length(IHist2)},
					  {<<"p_anal">>,length(IHist)}
					 ]
				};
			_ ->
				throw('no_sufficient_data')
		end
	catch 
		throw:no_sufficient_data ->
			lager:info("Car ~p, hour ~p: No sufficient data",[DevID, CHour]),
			{ok, [
				  {<<"sum">>,0},
				  {<<"diff">>,0},
				  {<<"events">>,[]},
				  {<<"t">>, Unixtime},
				  {<<"p_total">>,length(Data)},
				  {<<"p_valid">>,length(IHist2)},
				  {<<"p_anal">>,length(IHist)}
				 ]};
		throw:Ee ->
			lager:error("Error throw ~p in ~p hour ~p",[Ee,ExtInfo,CHour]),
			lager:error("Data ~p",[Data]),
			lists:map(fun(E)->
							  lager:error("At ~p",[E])
					  end,erlang:get_stacktrace()),
			throw(Ee);
		exit:Ee ->
			lager:error("Error exit ~p in ~p hour ~p",[Ee,ExtInfo,CHour]),
			lager:error("Data ~p",[Data]),
			lists:map(fun(E)->
							  lager:error("At ~p",[E])
					  end,erlang:get_stacktrace()),
			exit(Ee);
		error:Ee ->
			lager:error("Error ~p in ~p hour ~p",[Ee,ExtInfo,CHour]),
			lager:error("Data ~p",[Data]),
			lists:map(fun(E)->
							  lager:error("At ~p",[E])
					  end,erlang:get_stacktrace()),
			erlang:error(Ee);
		Ec:Ee ->
			lager:error("Error ~p:~p in ~p hour ~p",[Ec,Ee,ExtInfo,CHour]),
			lager:error("Data ~p",[Data]),
			lists:map(fun(E)->
							  lager:error("At ~p",[E])
					  end,erlang:get_stacktrace()),
			throw({Ec,Ee})
	end.

findev([],_,_,_,_,_) ->
	[];
findev([_],_,_,_,_,_) -> 
	[];
findev([A1,A2|Rest],Averages,LastEv,Thr,MinRF,MinDump) ->
	if A1#pi_fuel_pdi.prevc == 0 orelse
	   A2#pi_fuel_pdi.prevc == 0 orelse
	   A1#pi_fuel_pdi.postc == 0 orelse
	   A2#pi_fuel_pdi.postc == 0 ->
		   findev([A2|Rest],Averages,LastEv,Thr,MinRF,MinDump);
	   LastEv+Averages > A1#pi_fuel_pdi.dt ->
		   findev([A2|Rest],Averages,LastEv,Thr,MinRF,MinDump);
	   A1#pi_fuel_pdi.dt >= A2#pi_fuel_pdi.dt ->
		   findev([A2|Rest],Averages,LastEv,Thr,MinRF,MinDump);
	   true ->

		   D1=A1#pi_fuel_pdi.postsd/A1#pi_fuel_pdi.postc - A1#pi_fuel_pdi.prevsd/A1#pi_fuel_pdi.prevc,
		   D2=A2#pi_fuel_pdi.postsd/A2#pi_fuel_pdi.postc - A2#pi_fuel_pdi.prevsd/A2#pi_fuel_pdi.prevc,
		   Di=abs(D2-D1)*1000,
		   if Di>Thr/4 ->
				  lager:info("dt ~-5w val ~6w -> ~-6w  deriv (kilo) ~6w -> ~-6w  = ~w",
							 [
							  A2#pi_fuel_pdi.dt - A1#pi_fuel_pdi.dt,
							  round(A1#pi_fuel_pdi.value*100)/100,
							  round(A2#pi_fuel_pdi.value*100)/100,
							  round(D1*1000),
							  round(D2*1000),
							  round(Di)
							 ]);
			  true -> ok
		   end,

		   Event=if Di>Thr ->
						Vol=A2#pi_fuel_pdi.postsu/A2#pi_fuel_pdi.postc -
						A1#pi_fuel_pdi.prevsu/A1#pi_fuel_pdi.prevc,
						lager:info("Event ~p ~p",[A2#pi_fuel_pdi.dt, Vol]),
						if Vol > 0 andalso Vol> MinRF ->
							   [ {A2#pi_fuel_pdi.dt, Vol }];
						   Vol < 0 andalso -Vol > MinDump ->
							   [ {A2#pi_fuel_pdi.dt, Vol }];
						   true -> 
							   []
						end;
					true -> 
						[]
				 end,

		   Event++findev([A2|Rest],Averages,
						 case Event of 
							 [] -> LastEv;
							 _ -> A2#pi_fuel_pdi.dt
						 end ,Thr,MinRF,MinDump)
	end.

traverse([],Passed,_,_,_) ->
	Passed;
traverse([E1|Rest],[],Acc,AvgT,Dir) ->
	traverse(Rest,[E1],Acc,AvgT,Dir);
traverse([Cur|Rest],[Prev|Passed],Acc,AvgT,Dir) ->
	%lager:debug("~p~n~p ... ~n~p",[length(Acc),Prev,Cur]),
	CurT=Cur#pi_fuel_pdi.dt,
	PrevT=Prev#pi_fuel_pdi.dt,
	if Dir == a andalso PrevT >= CurT ->
		   traverse(Rest,[Prev|Passed],Acc,AvgT,Dir);
	   Dir == b andalso CurT >= PrevT ->
		   traverse(Rest,[Prev|Passed],Acc,AvgT,Dir);
	   true ->
		   MinT=CurT-AvgT,
		   MaxT=CurT+AvgT,
		   DxDt=(Cur#pi_fuel_pdi.value-Prev#pi_fuel_pdi.value) / (CurT-PrevT),
		   Acc1=lists:filter(fun({Xt,_,_}) ->
									 case Dir of
										 a -> Xt>=MinT;
										 b -> MaxT>=Xt
									 end
							 end,Acc)++[{Cur#pi_fuel_pdi.dt,DxDt,Cur#pi_fuel_pdi.value}],
		   {RCnt,RSum,USum}=lists:foldl(
							  fun({_,Dxt,Dvl},{Cnt,Sum,SU}) ->
									  {Cnt+1,Sum+Dxt,Dvl+SU}
							  end,
							  {0,0,0},
							  Acc1),
		   Cur1=case Dir of
					a ->
						Cur#pi_fuel_pdi{
						  prevc=RCnt,
						  prevsd=RSum,
						  prevsu=USum
						 };
					b ->
						Cur#pi_fuel_pdi{
						  postc=RCnt,
						  postsd=RSum,
						  postsu=USum
						 }
				end,

		   traverse(Rest,[Cur1,Prev|Passed],Acc1,AvgT,Dir)
	end.

