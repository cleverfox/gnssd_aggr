-module(agg_fuelmeter).

-export([process/3]).

process(Data,{_ExtInfo,_PrevAggregated},_Prev) ->
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
		  {<<"sum">>,Fuel}
		 ]}.



