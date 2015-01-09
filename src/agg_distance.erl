-module(agg_distance).

-export([process/2]).

process(Data,_ExtInfo) ->
	{ok, {somedata, [<<"Somevalue">>,Data]}}.



