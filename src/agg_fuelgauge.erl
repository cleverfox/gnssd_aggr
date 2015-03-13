-module(agg_fuelgauge).

-export([process/3]).

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


process(Data,{_ExtInfo,_PrevAggregated},_Prev) ->
	lager:info("Ext ~p",[_ExtInfo]),

	Averages=10,
	IHist=lists:filtermap(
			fun(DS) ->
					case proplists:get_value(dt,DS) of
						undefined ->
							false;
						DT ->
							case proplists:get_value(v_fuel,DS) of
								undefined ->
									false;
								FV ->
									{true,
									 #pi_fuel_pdi{ dt=DT, value=FV }
									}
							end
					end
			end, Data),
	%IH1=[ #pi_fuel_pdi{ dt=T, value=L } || {T,L} <- IHist ],
	T1=traverse(IHist,[],[],Averages,a),
	T2=traverse(T1,[],[],Averages,b),
	[First|_]=T2,
	[Last|_]=T1,
	TotalDiff=Last#pi_fuel_pdi.value - First#pi_fuel_pdi.value,

	Events=findev(T2,Averages,0,300),
	lager:info("FE ~p",[Events]),
	Changes=lists:foldl(fun({_,Amount},Sum) ->
						Sum+Amount
				end, 0, Events),


	lager:info("Delta ~p / ~p",[TotalDiff, TotalDiff - Changes]),

	{ok, [
		  {<<"sum">>,TotalDiff - Changes},
		  {<<"diff">>,TotalDiff},
		  {<<"events">>,[ {dt,EvT,amount,EvA} || {EvT,EvA} <-Events]}
		 ]}.

findev([],_,_,_) ->
	[];
findev([_],_,_,_) -> 
	[];
findev([A1,A2|Rest],Averages,LastEv,Thr) ->
	if A1#pi_fuel_pdi.prevc == 0 orelse
	   A2#pi_fuel_pdi.prevc == 0 orelse
	   A1#pi_fuel_pdi.postc == 0 orelse
	   A2#pi_fuel_pdi.postc == 0 ->
		   findev([A2|Rest],Averages,LastEv,Thr);
	   A1#pi_fuel_pdi.dt<LastEv+Averages ->
		   findev([A2|Rest],Averages,LastEv,Thr);
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
						if 
							abs(Vol) > 0.1 -> 
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
						 end ,Thr)
	end.

traverse([],Passed,_,_,_) ->
	Passed;
traverse([E1|Rest],[],Acc,AvgT,Dir) ->
	traverse(Rest,[E1],Acc,AvgT,Dir);
traverse([Cur|Rest],[Prev|Passed],Acc,AvgT,Dir) ->
	%lager:debug("~p~n~p ... ~n~p",[length(Acc),Prev,Cur]),
	CurT=Cur#pi_fuel_pdi.dt,
	MinT=CurT-AvgT,
	MaxT=CurT+AvgT,
	DxDt=(Cur#pi_fuel_pdi.value-Prev#pi_fuel_pdi.value) / (CurT-Prev#pi_fuel_pdi.dt),
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

	traverse(Rest,[Cur1,Prev|Passed],Acc1,AvgT,Dir).

