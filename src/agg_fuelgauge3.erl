-module(agg_fuelgauge3).

-export([process/4,findFirst/2,storename/0,testx/0]).

storename() ->
	agg_fuelgauge.

-record(pi_fuel_pdi, {
		  dt,
		  stop,
		  value,
		  prevsu=0,
		  prevc=0,
		  postsu=0,
		  postc=0,
		  event,
		  prevscu=0,
		  prevscc=0,
		  postscu=0,
		  postscc=0,
		  position
		 }).

r2pl(#pi_fuel_pdi{} = Rec) ->
	  lists:zip(record_info(fields, pi_fuel_pdi), tl(tuple_to_list(Rec))).

%recalc
testx() ->
	Filter={device,863,hour,{'$gt',399486-3,'$lt',399486+3}},
	C=mng:find(mongo,<<"devicedata">>,Filter,{'_id',1}),
	Res=mc_cursor:rest(C),
	mc_cursor:close(C),
	[ 
	 poolboy:transaction(ga_redis,
						 fun(W)-> 
								 K= <<(list_to_binary(mng:id2hex(ID)))/binary,":agg_fuelgauge3">>,
								 eredis:q(W,[ "lpush", <<"aggregate:express">>, K])
						 end) || {'_id',ID} <-Res ].  


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
	Averages=proplists:get_value(averages,MyC,
								 proplists:get_value("averages",MyC,
													 180
													)
								),
	Prefetch=proplists:get_value(prefetch,MyC,
								 proplists:get_value("prefetch",MyC,
													 Averages*4
													)
								),
	MinSP=proplists:get_value(min_speed,MyC,
							  proplists:get_value("min_speed",MyC,
												  3
												 ) 
							 ),
	ThrH=proplists:get_value(threshold_h,MyC,
							 proplists:get_value("threshold_h",MyC,
												 10
												) 
							),
	ThrL=proplists:get_value(threshold_l,MyC,
							 proplists:get_value("threshold_l",MyC,
												 2
												) 
							),
	EvThr={ThrH,ThrL},
	PH=try case proplists:get_value(fetchfun,ExtInfo) of
			   Fu when is_function(Fu) ->
				   BegTime=(CHour*3600)-Prefetch,
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
									Stop=case proplists:get_value(sp,DS) of
										   N when (is_integer(N) orelse is_float(N)) andalso N>MinSP ->
												 false;
											 _ -> true
										 end,
									{true,
									 #pi_fuel_pdi{ 
										dt=DT,
									   	value=FV,
										stop=Stop,
										position=proplists:get_value(position,DS)
									   }
									};
								_ -> false
							end
					end
			end,
	IHist1=lists:filtermap(HistProc, PH),
	IHist2=lists:filtermap(HistProc, Data),
	IHist=IHist1++IHist2,
	%lager:debug("IH ~p",[length(IHist)]),
	try case length(IHist) > 2 of
			true ->
				%IH1=[ #pi_fuel_pdi{ dt=T, value=L } || {T,L} <- IHist ],
				T1=traverse(IHist,[],[],[],Averages,a,EvThr,false),
				T2a=traverse(T1,[],[],[],Averages,b,EvThr,false),
				T2=traverse(T2a,false),
				%lager:info("T1 ~p",[T1]),
				%lager:info("T2 ~p",[T2]),
				First=findFirst(T2,CHour*3600),

				T2L=lists:map(fun(E) -> [<<"v3">>] ++ tl( erlang:tuple_to_list(E)) end, T2),
				%lager:info("T2: ~p",[ T2L ]),
				DumpFN= <<"/tmp/agg_fuel.",
								  (integer_to_binary(DevID))/binary,
								  "/agg_fuelgauge.dump.",
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
					%Events=findev(T2,Averages,0,Threshold,MinRF,MinDump),
					Events=findev3(T2,undefined,MinRF,MinDump),
				lager:debug("Car ~p Found fuel events ~p",[DevID,Events]),

				Changes=lists:foldl(fun({_,Amount,_,_,_},Sum) ->
											Sum+Amount
									end, 0, Events),

				poolboy:transaction(ga_redis,
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

				GetTF=fun(#pi_fuel_pdi{ dt=CDT, value=CFV }) ->
							  {CDT, CFV}
					  end,
				{Time1,Fuel1}=GetTF(hd(IHist2)),
				{Time2,Fuel2}=GetTF(lists:last(IHist2)),
				lager:info("Time ~p -> ~p, Fuel ~p -> ~p, Evs ~p",
						   [Time1,Time2,Fuel1,Fuel2, Events]),
				{ok, [
					  {<<"v">>,3},
					  {<<"sum">>,TotalDiff - Changes},
					  {<<"averages">>,Averages},
					  {<<"minrefuel">>,MinRF},
					  {<<"mindump">>,MinDump},
					  {<<"min_speed">>,MinSP},
					  {<<"threshold_h">>,ThrH},
					  {<<"threshold_l">>,ThrL},
					  {<<"diff">>,TotalDiff},
					  {<<"prefetch">>,Prefetch},
					  {<<"events">>,[ 
									 {dt,EvT,amount,EvA,t1,EvT1,t2,EvT2,position,EPos} ||
									 {EvT,EvA,EvT1,EvT2,EPos} <-Events, trunc(EvT2/3600) == CHour 
									]},
					  {<<"t">>, Unixtime},
					  {<<"dt1">>,Time1},
					  {<<"dt2">>,Time2},
					  {<<"fuel1">>,Fuel1},
					  {<<"fuel2">>,Fuel2}
					  %,
					  %{<<"p_total">>,length(Data)},
					  %{<<"p_valid">>,length(IHist2)},
					  %{<<"p_anal">>,length(IHist)}
					 ]
				};
			_ ->
				throw('no_sufficient_data')
		end
	catch 
		throw:no_sufficient_data ->
				lager:debug("Car ~p, hour ~p: No sufficient data",[DevID, CHour]),
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
			  throw(Ee)
		%exit:Ee ->
		%	  lager:error("Error exit ~p in ~p hour ~p",[Ee,ExtInfo,CHour]),
		%	  lager:error("Data ~p",[Data]),
		%	  lists:map(fun(E)->
		%						lager:error("At ~p",[E])
		%				end,erlang:get_stacktrace()),
		%	  exit(Ee);
		%error:Ee ->
		%	  lager:error("Error ~p in ~p hour ~p",[Ee,ExtInfo,CHour]),
		%	  lager:error("Data ~p",[Data]),
		%	  lists:map(fun(E)->
		%						lager:error("At ~p",[E])
		%				end,erlang:get_stacktrace()),
		%	  erlang:error(Ee);
		%Ec:Ee ->
		%	  lager:error("Error ~p:~p in ~p hour ~p",[Ec,Ee,ExtInfo,CHour]),
			  %lager:error("Data ~p",[Data]),
			  %lists:map(fun(E)->
			%					lager:error("At ~p",[E])
			%			end,erlang:get_stacktrace()),
			 % throw({Ec,Ee})
	end.

findev3([],_,_,_) ->
	[];
findev3([Cur|Rest],CurEv,MinRF,MinDump) ->
	case Cur#pi_fuel_pdi.event of
		false when CurEv=:=undefined ->
			findev3(Rest,CurEv,MinRF,MinDump);
		true when CurEv=/=undefined ->
			findev3(Rest,CurEv,MinRF,MinDump);
		true when Cur#pi_fuel_pdi.stop  -> %begin event
			findev3(Rest, Cur, MinRF,MinDump);
		true -> %looks like begin, but not stopped. ignore
			findev3(Rest, CurEv, MinRF,MinDump);
		false -> %fin event
			try
				T1=CurEv#pi_fuel_pdi.dt,
				T2=Cur#pi_fuel_pdi.dt,
				Pos=Cur#pi_fuel_pdi.position,
				F1=CurEv#pi_fuel_pdi.prevsu/CurEv#pi_fuel_pdi.prevc,
				F2=Cur#pi_fuel_pdi.postsu/Cur#pi_fuel_pdi.postc,
				Fd=F2-F1,
				%EvTime=trunc((T2-T1)/2)+T1,
				EvTime=T2,
				%[Cur#pi_fuel_pdi{event=true}|traverse(Rest,true)];
				if Fd>0 andalso Fd>MinRF ->
					  Event={ EvTime, Fd, T1, T2, Pos },
					  [Event|findev3(Rest, undefined, MinRF,MinDump)];
				   Fd<0 andalso -Fd>MinDump ->
					  Event={ EvTime, Fd, T1, T2, Pos },
					  [Event|findev3(Rest, undefined, MinRF,MinDump)];
				  true -> 
					  findev3(Rest, undefined, MinRF,MinDump)
				end
			catch error:badarith ->
					  findev3(Rest, undefined, MinRF,MinDump)
			end
	end.

traverse([],PrevPts,_,_,_,_,_,_) ->
%	lager:info("End ~p",[PrevPts]),
	PrevPts;
%traverse([Cur|Rest],[],Acc,AccL,AvgT,Dir,EvThr,A) ->
%	traverse(Rest,[Cur],Acc,AccL,AvgT,Dir,EvThr,A);
traverse([Cur|Rest],PrevPts,Acc,AccL,AvgT,Dir,{ThrH,ThrL},Act) ->
	CurT=Cur#pi_fuel_pdi.dt,
	PrevT=if length(PrevPts)==0 ->
				 undefined;
			 true ->
				Prev=hd(PrevPts), 
				Prev#pi_fuel_pdi.dt
		  end,
	Skip=case PrevT of
			 undefined -> 
				 false;
			 _ ->
				 case Dir of
					 a when PrevT >= CurT ->
						 true;
					 b when CurT >= PrevT ->
						 true;
					 _ -> 
						 false
				 end
		 end,
	if Skip ->
		   %lager:info("Skip ~p",[Cur#pi_fuel_pdi.dt]),
		   traverse(Rest,PrevPts,Acc,AccL,AvgT,Dir,{ThrH,ThrL},Act);
	   true ->
%		   lager:info("~p  ~p ...   ~p",[length(Acc),PrevPts,Cur]),
		   MinT=CurT-AvgT,
		   MaxT=CurT+AvgT,
		   %DxDt=(Cur#pi_fuel_pdi.value-Prev#pi_fuel_pdi.value) / (CurT-PrevT),
		   CurAcc={Cur#pi_fuel_pdi.dt,0,Cur#pi_fuel_pdi.value},
		   Acc1=lists:filter(fun({Xt,_,_}) ->
									 case Dir of
										 a -> Xt>=MinT;
										 b -> MaxT>=Xt
									 end
							 end,Acc),

		   AccL1=[CurAcc]++try {ListA,_}=lists:split(3, AccL), ListA
				 catch _:_ -> AccL
				 end,
		   AccL2=AccL1,
		   {RCnt,_RSum,USum}=lists:foldl(
							  fun({_,Dxt,Dvl},{Cnt,Sum,SU}) ->
									  {Cnt+1,Sum+Dxt,Dvl+SU}
							  end,
							  {0,0,0},
							  Acc1),
		   {R2Cnt,U2Sum}=lists:foldl(
						   fun({_,_,Dvl},{Cnt,Sum}) ->
								   {Cnt+1,Dvl+Sum}
						   end,
						   {0,0},
						   AccL1),
		   {RACnt,UASum} = if RCnt > 1 orelse R2Cnt < 1 ->
								  {RCnt,USum};
								 true  -> 
								  {R2Cnt, U2Sum }
						   end,
	
		   {Cur1,NA}=case Dir of
						 a ->
							 {
							  Cur#pi_fuel_pdi{
								prevc=RACnt,
								prevsu=UASum
							   },
							  Act
							 };
						 b ->
							 Ev=case Cur#pi_fuel_pdi.prevc>0 andalso RACnt>0 of
									false -> 
										undefined;
									true ->
										Pred=Cur#pi_fuel_pdi.prevsu/Cur#pi_fuel_pdi.prevc,
										Posd=UASum/RACnt,
										Diff=Posd-Pred,
										ADiff=abs(Diff),
										if ADiff >= ThrH ->
											   true;
										   ADiff > ThrL andalso Act ->
											   true;
										   ADiff > ThrL ->
											   possible;
										   true ->
											   false
										end
								end,
							 {
							  Cur#pi_fuel_pdi{
								postc=RACnt,
								postsu=UASum,
								event=Ev
							   },
							  Ev =:= true
							 }
					 end,
%		   if Dir == b ->
%				  %lager:info("Point ~p", [Cur]),
%				  lager:info("dir ~p Len1 ~p Len2 ~p RA ~p, R2 ~p",
%							 [Dir,length(AccL1),length(AccL2),{RACnt,UASum},{R2Cnt,U2Sum}]),
%				  lager:info("Pt ~p ~p",[Cur1#pi_fuel_pdi.postc,Cur1#pi_fuel_pdi.postsu]),
%				  ok;
%			  true ->
%				  ok
%		   end,

		   traverse(Rest,[Cur1|PrevPts],Acc1 ++ [CurAcc], AccL2, AvgT, Dir,{ThrH,ThrL},NA)
	end.

traverse([],_A) ->
	[];
traverse([Cur|Rest],A) ->
	case Cur#pi_fuel_pdi.event of
		true ->
			[Cur|traverse(Rest,true)];
		possible when A ->
			[Cur#pi_fuel_pdi{event=true}|traverse(Rest,true)];
		_ ->
			[Cur#pi_fuel_pdi{event=false}|traverse(Rest,false)]
	end.



