%%%-------------------------------------------------------------------
%%% @author Vladimir Goncharov <devel@viruzzz.org>
%%% @copyright (C) 2014, Vladimir Goncharov
%%% @doc
%%%
%%% @end
%%% Created :  16 Nov 2014 by Vladimir Goncharov
%%%-------------------------------------------------------------------
-module(gnss_aggr).
-author("Vladimir Goncharov").
-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1, init/1]).

-define(MAX_RESTART,    10).
-define(MAX_TIME,      60).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

start() ->
	application:ensure_all_started(gnss_aggr).

start(_StartType, _StartArgs) ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
	ok.

init([]) ->
	{RedisHost,RedisPort} = case application:get_env(redis) of 
					{ok, {RHost, RPort} } ->
						{RHost,RPort};
					_ ->
						{"127.0.0.1",6379}
				end,
	MongoCfg = case application:get_env(mongodb) of
				   {ok,X} when is_list(X) -> X;
				   _ -> 
					   lager:error("Can't get mongoDB configuration"),
					   []
			   end,
	{ok,
	 {_SupFlags = {one_for_one, ?MAX_RESTART, ?MAX_TIME},
	  [
	   {   ga_pool_redis,
	       {poolboy,start_link,[
				    [{name,{local,ga_redis}},
				     {worker_module,eredis},
				     {size,3},
				     {max_overflow,20}
				    ],
				    [ {host, RedisHost}, 
				      {port, RedisPort}
				    ] 
				   ]}, 
	       permanent, 5000, worker,
	       [poolboy,eredis]
	   },
	   {   ga_pool_mongo,
		   {poolboy,start_link,[
								[{name,{local,ga_mongo}},
								 {worker_module,mc_worker},
								 {size,3},
								 {max_overflow,20}
								],
								MongoCfg
							   ]},
		   permanent, 5000, worker, 
		   [poolboy,mc_worker]
	   },
%	   
%	   {   ga_pool_postgres,
%	       {poolboy,start_link,[
%				    [{name,{local,ga_postgres}},
%				     {worker_module,pgsql_worker},
%				     {size,1},
%				     {max_overflow,3}
%				    ],
%					[
%					]
%				   ]},
%	       permanent, 5000, worker, 
%		   [poolboy]
%	   },
	   {   ga_aggregator_sup,
	       {aggregator_sup,start_link, [ ] },
	       permanent, 2000, supervisor, []
	   },
	   {   ga_aggregator_dispatcher,
	       {aggregator_dispatcher,start_link, [
	       RedisHost, RedisPort, "aggregate"
	        ] },
	       permanent, 2000, supervisor, []
	   }
	  ]
	 }
	}.


