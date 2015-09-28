-module(agg_fuelgauge2).

-include_lib("eunit/include/eunit.hrl").

-export([process/4,findFirst/2,storename/0,test_histfilter/0,testx/0]).

storename() ->
	agg_fuelgauge.

-record(pi_fuel_pdi, {
		  dt,
		  do,
		  df,
		  ao,
		  stop,
		  value,
		  prevsu=0,
		  prevc=0,
		  postsu=0,
		  postc=0,
		  position
		 }).

r2pl(#pi_fuel_pdi{} = Rec) ->
	  lists:zip(record_info(fields, pi_fuel_pdi), tl(tuple_to_list(Rec))).

%recalc
testx() ->
	Get=fun(D,H1,H2) ->
				Filter={device,D,hour,{'$gte',H1,'$lte',H2}},
				C=mng:find(mongo,<<"devicedata">>,Filter,{'_id',1}),
				Res=mc_cursor:rest(C),
				mc_cursor:close(C),
				Res
		end,

	%Filter={device,863,hour,{'$gt',399486,'$lt',399486+2}},
	%Res=Get(428,398938,1436367600/3600)++Get(863,399486,399486+2),
	%Res=Get(428,398938,398999)++Get(863,399487,399487),
	%Res=Get(428,398985,398985),
	%Res=Get(428,398925,399333),
	Res=Get(428,1436180400/3600,1436180400/3600),
	%Res=Get(863,399487,399487),
	%Filter={device,428,hour,398985},
	[ 
	 poolboy:transaction(redis,
						 fun(W)-> 
								 LID=mng:id2hex(ID),
								 K= <<(list_to_binary(LID))/binary,":agg_fuelgauge2">>,
								 eredis:q(W,[ "lpush", <<"aggregate:express">>, K]),
								 eredis:q(W,[ "publish", <<"aggregate">>, K]),
								 LID
						 end) || {'_id',ID} <-Res 
	].  

findFirst([],_) ->
		throw('no_sufficient_data');
findFirst([L|Rest],Time) ->
	case L#pi_fuel_pdi.dt>=Time of
		true -> 
			L;
		_ -> 
			findFirst(Rest,Time)
	end.

hist_odo([], _SO) ->
	[];

hist_odo([#pi_fuel_pdi{ do = DO } = H|Rest], SO) ->
	SO2=DO + SO,
	[H#pi_fuel_pdi{ ao = SO2 } | hist_odo(Rest, SO2)].

process(Data,{ExtInfo,_PrevAggregated},_Prev,Config) ->
	{MSec,Sec,_} = now(),
	Unixtime=MSec*1000000 + Sec,
	CHour=proplists:get_value(hour,ExtInfo),
	DevID=proplists:get_value(device,ExtInfo),
	AllC=maps:get(aggregators,Config,#{}),
	MyC=maps:get(?MODULE,AllC,[]),
	lager:debug("Car ~p My Config ~p",[DevID,MyC]),
	MinRF=proplists:get_value(minrefuel,MyC,
							  proplists:get_value("minrefuel",MyC,
												  10
												 )
							 ),
	MinDump=proplists:get_value(mindump,MyC,
							  proplists:get_value("mindump",MyC,
												  5
												 )
							 ),
	Averages_m=proplists:get_value(averages_m,MyC,
								 proplists:get_value("averages_m",MyC,
													 50
													)
								),
	PMH=proplists:get_value(prefetch_maxhours,MyC,
								 proplists:get_value("prefetch_maxhours",MyC,
													 48
													) 
								 ),
	PDist=proplists:get_value(prefetch_dist,MyC,
								 proplists:get_value("prefetch_dist",MyC,
													 1000
													) 
								 ),
	OdoS=case proplists:get_value(odometer_name,MyC,
							   proplists:get_value("odometer_name",MyC,
												   "softodometer"
												  ) 
							  ) of
			 OdoS1 when is_binary(OdoS1) -> binary_to_atom(OdoS1 , latin1);
			 OdoS1 when is_list(OdoS1) -> list_to_atom(OdoS1);
			 OdoS1 when is_atom(OdoS1) -> OdoS1
		 end,
	MinSP=proplists:get_value(min_speed,MyC,
							  proplists:get_value("min_speed",MyC,
												  3
												 ) 
							 ),

	OdoName=try list_to_existing_atom(OdoS)
		catch _:_ -> OdoS
		end,
	IHist2=lists:filtermap(fun(DS) ->
								histproc(DS,OdoName, MinSP)
						 end, Data),
	case length(IHist2)<2 of
		true ->
				throw('no_sufficient_data');
		_ -> ok
	end,

	IHist1=try case proplists:get_value(fetchfun,ExtInfo) of
				   Fu when is_function(Fu) ->
					   prefetch(Fu, CHour-1, PMH, PDist, OdoName, MinSP)
			   end
		   catch _:_ -> []
		   end,
	%lager:debug("Car ~p PrevHr prepend ~p",[DevID,IHist1]),
	IHist=IHist1++IHist2,
%	lager:error("Car ~p diff prepend ~p",[DevID,IHist]),

	try case length(IHist) > 2 of
			true ->
				IHistO=hist_odo(IHist,0),
				%IH1=[ #pi_fuel_pdi{ dt=T, value=L } || {T,L} <- IHist ],
				T1=traverse(IHistO,[],[],Averages_m,a),
				T2=traverse(T1,[],[],Averages_m,b),
				%IHist=differentiation(IHist1++IHist2),

				T2L=lists:map(fun(E) -> r2pl(E) end, T2),
				%lager:debug("T2: ~p",[ T2L ]),
				DumpFN= <<"/tmp/agg_fuel.",
								  (integer_to_binary(DevID))/binary,
								  "/agg_fuelgauge2.dump.",
								  (integer_to_binary(DevID))/binary,
								  ".",
								  (integer_to_binary(CHour))/binary,
								  ".json">>,
				try
					file:write_file(DumpFN, 
									iolist_to_binary(mochijson2:encode( T2L )))

					catch _:_ -> 
						  lager:info("Can't save to ~p",[DumpFN]),
						  ok
				end,

				First=findFirst(T2,CHour*3600),

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
				Events=findev(T2,Averages_m,0,MinRF,MinDump,none),
				lager:debug("Car ~p Found fuel events ~p",[DevID,Events]),

				Changes=lists:foldl(fun({_,Amount,_,_,_},Sum) ->
											Sum+Amount
									end, 0, Events),

				poolboy:transaction(redis,
									fun(W)->
											DKey="device:fuel:"++integer_to_list(DevID)++":"++integer_to_list(CHour)++":",
											if 
												length(Events) > 0 ->
													KeyH=DKey++"events",
													RLS=lists:foldl(
														  fun({TS,Amount,_,_,_},Sum) ->
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

				GetTF=fun(#pi_fuel_pdi{ dt=CDT, df=CFV }) ->
							  {CDT, CFV}
					  end,
				{Time1,Fuel1}=GetTF(hd(IHist2)),
				{Time2,Fuel2}=GetTF(lists:last(IHist2)),
				lager:info("Time ~p -> ~p, Fuel ~p -> ~p",
						   [Time1,Time2,Fuel1,Fuel2]),
				{ok, [
					  {<<"v">>,2},
					  {<<"sum">>,TotalDiff - Changes},
					  {<<"averages_m">>,Averages_m},
					  {<<"minrefuel">>,MinRF},
					  {<<"mindump">>,MinDump},
					  {<<"diff">>,TotalDiff},
					  {<<"prefetch_dist">>,PDist},
					  {<<"prefetch_maxhours">>,PMH},
					  {<<"events">>,[ 
									 {dt,EvT,amount,EvA,t1,EvT1,t2,EvT2,position,EPos} ||
									 {EvT,EvA,EvT1,EvT2,EPos} <-Events, trunc(EvT2/3600) == CHour 
									]},
					  {<<"t">>, Unixtime},
					  {<<"dt1">>,Time1},
					  {<<"dt2">>,Time2},
					  {<<"fuel1">>,Fuel1},
					  {<<"fuel2">>,Fuel2}
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
			Stack=erlang:get_stacktrace(),
			lager:error("Error throw ~p in ~p hour ~p",[Ee,ExtInfo,CHour]),
			lager:error("Data ~p",[Data]),
			lists:map(fun(E)->
							  lager:error("At ~p",[E])
					  end,Stack),
			throw(Ee);
		exit:Ee ->
			Stack=erlang:get_stacktrace(),
			lager:error("Error exit ~p in ~p hour ~p",[Ee,ExtInfo,CHour]),
			lager:error("Data ~p",[Data]),
			lists:map(fun(E)->
							  lager:error("At ~p",[E])
					  end,Stack),
			exit(Ee);
		error:Ee ->
			Stack=erlang:get_stacktrace(),
			lager:error("Error ~p in ~p hour ~p",[Ee,ExtInfo,CHour]),
			lager:error("Data ~p",[Data]),
			lists:map(fun(E)->
							  lager:error("At ~p",[E])
					  end,Stack),
			erlang:error(Ee);
		Ec:Ee ->
			Stack=erlang:get_stacktrace(),
			lager:error("Error ~p:~p in ~p hour ~p",[Ec,Ee,ExtInfo,CHour]),
			lager:error("Data ~p",[Data]),
			lists:map(fun(E)->
							  lager:error("At ~p",[E])
					  end,Stack),
			throw({Ec,Ee})
	end.

findev([],_,_,_,_,_) ->
	[];
findev([_],_,_,_,_,_) -> 
	[];
findev([A1,A2|Rest],Averages,LastEv,MinRF,MinDump,EvB) ->
	if A1#pi_fuel_pdi.prevc == 0 orelse
	   A2#pi_fuel_pdi.prevc == 0 orelse
	   A1#pi_fuel_pdi.postc == 0 orelse
	   A2#pi_fuel_pdi.postc == 0 ->
		   findev([A2|Rest],Averages,LastEv,MinRF,MinDump,EvB);
	   LastEv+Averages > A1#pi_fuel_pdi.ao -> %make event shadow
		   findev([A2|Rest],Averages,LastEv,MinRF,MinDump,EvB);
%	   A1#pi_fuel_pdi.dt >= A2#pi_fuel_pdi.dt ->
%		   findev([A2|Rest],Averages,LastEv,MinRF,MinDump,EvB);
	   true ->
		   Vol=A2#pi_fuel_pdi.postsu/A2#pi_fuel_pdi.postc - A1#pi_fuel_pdi.prevsu/A1#pi_fuel_pdi.prevc,
		   
		   EvB2=if Vol > 0 andalso Vol*2> MinRF andalso (EvB == none orelse element(1,EvB)==1) ->
						%lager:debug("Event ~p ~p",[A2#pi_fuel_pdi.dt, Vol]),
						case EvB of
							{1, E1, _, PVol, PStop} ->
								{1, E1, A2, 
								 if PVol > Vol -> PVol; true -> Vol end,
								 A1#pi_fuel_pdi.stop or A2#pi_fuel_pdi.stop or PStop
								};
							_ ->
								{1, A1, A2, Vol, A1#pi_fuel_pdi.stop or A2#pi_fuel_pdi.stop}
						end;
					Vol < 0 andalso -Vol*2 > MinDump andalso (EvB == none orelse element(1,EvB)==-1) ->
						%lager:debug("Event ~p ~p",[A2#pi_fuel_pdi.dt, Vol]),
						case EvB of
							{-1, E1, _, PVol, PStop} ->
								{-1, E1, A2, 
								 if PVol > Vol -> PVol; true -> Vol end,
								 A1#pi_fuel_pdi.stop or A2#pi_fuel_pdi.stop or PStop
								};
							_ ->
								{-1, A1, A2, Vol, A1#pi_fuel_pdi.stop or A2#pi_fuel_pdi.stop}
						end;
					true -> 
					   %lager:debug("noEvent ~p ~p",[A2#pi_fuel_pdi.dt, Vol]),
						none 
				 end,
		   {Event, NextEv} = case EvB of
								 {_, Ev1, Ev2, _MaxVol, Stopped} when EvB2 == none ->
									 T1=Ev1#pi_fuel_pdi.dt,
									 T2=Ev2#pi_fuel_pdi.dt,
									 Volume=(Ev2#pi_fuel_pdi.postsu/Ev2#pi_fuel_pdi.postc) - 
									 (Ev1#pi_fuel_pdi.prevsu/Ev1#pi_fuel_pdi.prevc),
									 EvTime=T2,
									 {
									  if 
										  Volume > 0 andalso Volume > MinRF andalso Stopped ->
											  [
											   { EvTime, Volume, T1, T2, Ev2#pi_fuel_pdi.position }
											  ];
										  Volume < 0 andalso -Volume > MinDump andalso Stopped ->
											  [
											   { EvTime, Volume, T1, T2, Ev2#pi_fuel_pdi.position }
											  ];
										  true -> []
									  end,
									  Ev2#pi_fuel_pdi.ao
									 };
								 _ ->
									 {[],LastEv}

		   end,
		   if Event =/= [] ->
				  lager:info("Ev ~p",[Event]);
			  true -> ok
		   end,

		   Event++findev([A2|Rest],Averages, NextEv, MinRF,MinDump,EvB2)
	end.

traverse([],Passed,_,_,_) ->
	Passed;
traverse([E1|Rest],[],Acc,AvgD,Dir) ->
	traverse(Rest,[E1],Acc,AvgD,Dir);
traverse([Cur|Rest],[Prev|Passed],Acc,AvgD,Dir) ->
	%lager:debug("~p~n~p ... ~n~p",[length(Acc),Prev,Cur]),
	CurO=Cur#pi_fuel_pdi.ao,
	PrevO=Prev#pi_fuel_pdi.ao,
	if Dir == a andalso PrevO >= CurO ->
		   traverse(Rest,[Prev|Passed],Acc,AvgD,Dir);
	   Dir == b andalso CurO >= PrevO ->
		   traverse(Rest,[Prev|Passed],Acc,AvgD,Dir);
	   true ->
		   MinO=CurO-AvgD,
		   MaxO=CurO+AvgD,
		   Acc1=lists:filter(fun({Xt,_}) ->
									 case Dir of
										 a -> Xt>=MinO;
										 b -> MaxO>=Xt
									 end
							 end,Acc),
		   Acc1b = if length(Acc) > 0 ->
						  case Acc1 of 
							  [] -> [hd(Acc)];
							  _ -> Acc1
						  end;
					  true -> Acc
				   end,

		   Acc2=[{Cur#pi_fuel_pdi.ao,Cur#pi_fuel_pdi.value}|Acc1],
		   %lager:info("Dir ~p AD ~p Acc ~p ~p",[Dir, AvgD, Acc, Acc1]),
		   {RCnt,USum}=lists:foldl(
							  fun({_,Dvl},{Cnt,SU}) ->
									  {Cnt+1,Dvl+SU}
							  end,
							  {0,0},
							  Acc1b),
		   Cur1=case Dir of
					a ->
						Cur#pi_fuel_pdi{
						  prevc=RCnt,
						  prevsu=USum
						 };
					b ->
						Cur#pi_fuel_pdi{
						  postc=RCnt,
						  postsu=USum
						 }
				end,

		   traverse(Rest,[Cur1,Prev|Passed],Acc2,AvgD,Dir)
	end.

%differentiation([]) ->
%	[];
%differentiation([_]) ->
%	[];
%differentiation([Prev,Next|Rest]) ->
%	%lager:info("Prev ~p, Next ~p",[Prev,Next]),
%	if is_integer(Next#pi_fuel_pdi.do) orelse is_float(Next#pi_fuel_pdi.do) ->
%		   DO=(Next#pi_fuel_pdi.do)+0.000001,
%		   DF=Next#pi_fuel_pdi.value-Prev#pi_fuel_pdi.value,
%		   Deriv=DF/DO,
%		   [
%			Next#pi_fuel_pdi{ value=Deriv } 
%			|
%			differentiation([Next|Rest])
%		   ];
%	   true ->
%		   differentiation([Prev|Rest])
%	end.

prefetch(_FFu, _Hour, 0, _Dist, _, _MinSP) -> 
	[];
prefetch(_FFu, _Hour, _HoursL, Dist, _, _MinSP) when 0>=Dist -> 
	[];
prefetch(FFu, Hour, HoursL, Dist, OdoName, MinSP) ->
	%lager:info("prefetch(~p,~p,~p,~p,~p,~p)",[FFu, Hour, HoursL, Dist, OdoName,MinSP]),

	case FFu(Hour) of
		nodocument ->
			prefetch(FFu,Hour-1,HoursL-1,Dist, OdoName, MinSP);
		PData when is_list(PData) ->
			Data=proplists:get_value(data,PData),
			lager:debug("Prefetch ~p HL ~p DL ~p ",[
										 calendar:gregorian_seconds_to_datetime((Hour*3600)+62167230000),
										 HoursL,
										 Dist
										]),

			RD=lists:reverse(Data),
			{RHist, Dist2} = histfilter(RD, Dist, OdoName),
			Hist=lists:filtermap(fun(DS) ->
										 histproc(DS,OdoName, MinSP)
								 end, lists:reverse(RHist)),
			prefetch(FFu,Hour-1,HoursL-1,Dist2, OdoName, MinSP) ++ Hist
	end.

histfilter([],Dist, _OdoName) ->
	{[], Dist} ;
histfilter(_,Dist, _OdoName) when 0>=Dist ->
	{[], Dist} ;
histfilter([DS|Rest],Dist, OdoName) ->
	case proplists:get_value(dt,DS) of
		undefined ->
			histfilter(Rest,Dist, OdoName);
		_ ->
			case proplists:get_value(v_fuel,DS) of
				0 -> 
					histfilter(Rest,Dist, OdoName);
				FV when is_float(FV) orelse is_integer(FV)->
					OdoV=case proplists:get_value(OdoName,DS) of
							 X when is_integer(X) orelse is_float(X) ->
								 X;
							 _ -> 
								 0
						 end,

					{NH, ND} = histfilter(Rest,Dist-OdoV, OdoName),
					{[DS|NH], ND};
				_ -> %undefined
					histfilter(Rest,Dist, OdoName)
			end
	end.

histproc(DS, OdoName, MinSP) ->
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
					OdoV=case proplists:get_value(OdoName,DS) of
							 X when is_integer(X) orelse is_float(X) ->
								 X;
							 _ -> 0
						 end,
					Stop=case proplists:get_value(sp,DS) of
							 N when (is_integer(N) orelse is_float(N)) andalso N>MinSP ->
								 false;
							 _ -> true
						 end,
					{true,
					 #pi_fuel_pdi{ 
						dt=DT, 
						value=FV, 
						do=OdoV, 
						df=FV,
						stop=Stop orelse OdoV<10,
						position=proplists:get_value(position,DS)
					   }
					};
				_ -> false
			end
	end.

agg_fuelgauge2_test_() ->
	[
	 ?_assert(ok =:= ok),
	 ?_assert(test_histfilter())

	].


test_histfilter() ->
	Data=[
		  [ {dt, 1},   {v_fuel, 100},  {softodometer, 10}  ],
		  [ {dt, 10},  {v_fuel, 100},  {softodometer, 10}  ],
		  [ {dt, 20},  {v_fuel, 95},   {softodometer, 10}  ],
		  [ {dt, 30},  {v_fuel, 95},   {softodometer, 90}  ],
		  [ {dt, 40},  {v_fuel, 95},   {softodometer, 130} ],
		  [ {dt, 50},  {v_fuel, 94.8}, {softodometer, 190} ],
		  [ {dt, 150}, {v_fuel, 94},   {softodometer, 990} ],
		  [ {dt, 200}, {v_fuel, 93.5}, {softodometer, 990} ],
		  [ {dt, 300}, {v_fuel, 93.5}, {softodometer, 990} ],
		  [ {dt, 400}, {v_fuel, 93.5}, {softodometer, 990} ],
		  [ {dt, 500}, {v_fuel, 93.5}, {softodometer, 990} ]
		 ],
	{Proc,Dist}=histfilter(Data, 310, softodometer),
	lists:foreach(fun(X) ->
						  lager:info("Test ~p",[ X ])
				  end, Proc),
	lager:info("Dist ~p",[ Dist ]),


	true.



