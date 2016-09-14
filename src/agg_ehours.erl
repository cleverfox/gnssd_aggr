-module(agg_ehours).

-export([process/4]).

process(Data,{ExtInfo,_PrevAggregated},_Prev,Config) ->
	AllC=maps:get(aggregators,Config,#{}),
	MyC=maps:get(?MODULE,AllC,[]),
	lager:info("My config ~p~n ~p",[MyC,ExtInfo]),
	Hour=proplists:get_value(hour,ExtInfo,0),
	T1=(Hour)*3600,
	T2=(Hour+1)*3600,
	Sense=proplists:get_value("sense",MyC,[]),

	{_,_,Sum}=lists:foldl(fun(Element,{Pre,PrevT,TimeSumm})->
						CurT=proplists:get_value(dt,Element),
						if CurT>=T1 andalso T2>CurT ->
							   case Pre of 
								   undefined ->
									   lager:debug("E ~p",[CurT]),
									   {Element,CurT,TimeSumm};
								   _ ->
									   Run=is_run(Element,Pre,Sense),
									   lager:debug("E: ~p ~p + ~p",[Run,CurT,CurT-PrevT]),
									   TSum=if Run ->
												   TimeSumm+CurT-PrevT;
											   true -> TimeSumm
											end,
									   {Element,CurT,TSum}
							   end;
						   CurT < T1 ->
							   lager:debug("noskip ~p",[CurT]),
							   {Element, CurT, TimeSumm};
						   true ->
							   lager:debug("skip ~p",[CurT]),
							   {Pre, PrevT, TimeSumm}
						end
				end,{undefined,0,0},Data),
	lager:info("Config my  ~p",[MyC]),
	{MSec,Sec,_} = now(),
	lager:info("Sum ~p",[Sum]),
	{ok, [
		  {<<"t">>, MSec*1000000 + Sec},
		  {<<"sum">>, Sum}
		 ]}.

to_atom(B) when is_atom(B) -> 
	B;
to_atom(B) when is_list(B) -> 
	try 
		list_to_existing_atom(B)
	catch 
		_:_ -> B
	end;
to_atom(B) when is_binary(B) ->
	to_atom(binary_to_list(B));
to_atom(B) ->
	B.

is_run(E,P,[[<<"change">>,Var0]|Rest]) ->
	Var=to_atom(Var0),
	CurVal=proplists:get_value(Var,E),
	PreVal=proplists:get_value(Var,P),
	if CurVal =/= PreVal ->
		   true;
	   true -> 
		   is_run(E,P,Rest)
	end;

is_run(E,P,[[<<"level">>,Var0,Low,High]|Rest]) ->
	Var=to_atom(Var0),
	CurVal=proplists:get_value(Var,E),
	case CurVal of 
		M when is_integer(M) orelse is_float(M) ->
			LOK=Low  == null orelse M>= Low,
			HOK=High == null orelse High >= M,
			if HOK andalso LOK ->
				   true;
			   true ->
				   is_run(E,P,Rest)
			end;
		_ ->  %undefined, or something other
			is_run(E,P,Rest)
	end;

is_run(_,_,[]) ->
	false.

