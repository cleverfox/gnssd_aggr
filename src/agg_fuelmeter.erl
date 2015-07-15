-module(agg_fuelmeter).

-export([process/4]).

process(Data,{_ExtInfo,_PrevAggregated},_Prev,_Config) ->
	{MSec,Sec,_} = now(),
	Fx = fun(Elm,{FSum}) ->
				 case proplists:get_value(v_tfuel,Elm) of
					 I when is_integer(I) ->
						 {FSum+I};
					 I when is_float(I) ->
						 {FSum+I};
					 _ -> {FSum}
				 end
		 end,
	{Fuel}=lists:foldl(Fx, {0}, Data),
	{ok, [
		  {<<"sum">>,Fuel},
		  {<<"t">>, MSec*1000000 + Sec}
		 ]}.

