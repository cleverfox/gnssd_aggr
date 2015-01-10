-module(agg_distance2).

-export([process/3]).

process(Data,{_ExtInfo,_PrevAggregated},_Prev) ->
	Fx = fun(Elm,{Min,Max,MaxS,PPos,PathSum}) ->
				 %[{dt,1420722010},{position,[37.59247079869102,51.620521248073814]},{dir,266.4217921976619},{sp,0},{v_fuel,25.10293735686435},{v_odometer,9308850.076139404}]
				 Odometer=proplists:get_value(v_odometer,Elm),
				 Speed=proplists:get_value(sp,Elm),
				 Pos=proplists:get_value(position,Elm),
				 MaxS1 = case {Speed, MaxS} of 
							 {undefined, _} -> MaxS;
							 {_, undefined} -> Speed;
							 _ when Speed > MaxS -> Speed;
							 _ -> MaxS
						 end,
				 {Min1,Max1} = case Odometer of 
								   undefined -> {Min,Max};
								   _ ->
									   {case Min of
											undefined -> Odometer;
											X when Odometer < X -> Odometer;
											_ -> Min
										end,
										case Max of
											undefined -> Odometer;
											X when Odometer > X -> Odometer;
											_ -> Max
										end}
							   end,
				 Dist=case Pos of 
						  PPos -> 0;
						  [Lon,Lat] -> case PPos of 
										   [Lon1,Lat1] ->
											   {_Az,XDist} = gpstools:sphere_inverse({Lon,Lat},{Lon1,Lat1}),
											   XDist;
										   _ -> 0
									   end;
						  _ -> 0
					  end,

			{Min1,Max1,MaxS1,Pos,PathSum+Dist}
		 end,
	{MSec,Sec,_} = now(),
	{OMin0, OMax, MaxS, _, PathLen}=lists:foldl(Fx, {undefinded, 0, 0, undefined, 0}, Data),
	OMin=case OMin0 of
			 Num when is_integer(Num) -> Num;
			 Num when is_float(Num) -> Num;
			 _ -> 0
		 end,

	%lager:info("Ext ~p",[ExtInfo]),
	%lager:info("Data ~p",[Data]),
	
	{ok, [
		  {<<"sum_v_odometer2">>,OMax-OMin}, 
		  {<<"pathlen2">>, PathLen*1000}, 
		  {<<"max_speed2">>, MaxS},
		  {<<"t">>, MSec*1000000 + Sec}
		 ]}.



