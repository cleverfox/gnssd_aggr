-module(aggregator_dispatcher).

-behaviour(gen_server).

%% API functions
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {redispid,
				chan,
				max_worker=50,
				max_express=20,
				timer
			   }).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Host, Port, Chan) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Host, Port, Chan], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Host, Port, Chan]) ->
	{ok, Pid} = eredis_sub:start_link(Host, Port, ""),
	lager:info("Eredis up ~p: ~p:~p",[Pid,Host,Port]),
	eredis_sub:controlling_process(Pid),
	eredis_sub:subscribe(Pid, [Chan]),
	lager:info("Eredis up ~p subscribe ~p",[Pid,Chan]),
	{ok, #state{
			redispid=Pid,
			chan=Chan,
			timer=erlang:send_after(10000,self(),run_queue)
		   }
	}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({finished, _}, State) ->
	handle_cast(run_queue, State);
	%{noreply, State};

handle_cast(run_queue, State) ->
	case State#state.timer of 
		undefined -> ok;
		_ -> erlang:cancel_timer(State#state.timer)
	end,
	{AllowNorm,AllowExpress}=case proplists:get_value(workers,supervisor:count_children(aggregator_sup)) of
		undefined -> 
			lager:error("Can't get worker count"),
			false;
		M when is_integer(M) ->
									 %lager:info("Workers ~p",[M]),
								 { 
								  State#state.max_worker > M,
								  State#state.max_worker+State#state.max_express > M
								 }
		  end,
	lager:debug("Allow run normal queue ~p, express ~p",[AllowNorm,AllowExpress]),
	S2=case {AllowExpress,AllowNorm} of
		   {false, false} -> 
			   State#state{timer=erlang:send_after(10000,self(),run_queue)};
		   { _ , _ } -> 
			   ExpressFun=fun(Worker) -> 
							  eredis:q(Worker, [ "rpop", "aggregate:express" ])
					  end,
			   {ok, ExpressID}=poolboy:transaction(ga_redis, ExpressFun),
			   L=case ExpressID of 
					 undefined ->  %non express
						 case AllowNorm of
							 false ->
								 false;
							 _ ->
								 NormalFun=fun(Worker) -> 
												   eredis:q(Worker, [ "rpop", "aggregate:devicedata" ])
										   end,
								 {ok, NormalJSON}=poolboy:transaction(ga_redis, NormalFun),
								 case NormalJSON of 
									 undefined -> false;
									 _ -> 
										 %lager:info("Normal JS ~p",[NormalJSON]),
										 try mochijson2:decode(NormalJSON) of
											 {struct,List} when is_list(List) ->
												 Key=mng:proplisttom(List),
												 % Non-Express
												 Tasks=default, %[agg_distance,agg_fuelmeter,agg_fuelgauge], 
												 case supervisor:start_child(aggregator_sup,[Key,Tasks]) of
													 {ok, Pid} -> 
														 lager:debug("Data aggregator ~p runned ~p",[Key, Pid]),
														 true;
													 {error, Err} -> 
														 lager:error("Can't run data aggregator: ~p",[Err]),
														 error
												 end;
											 _Any -> 
												 lager:error("Can't parse source ~p",[NormalJSON]),
												 error
										 catch
											 error:Err ->
												 lager:error("Can't parse source ~p",[Err]),
												 error
										 end
								 end
						 end;
					 EID0 -> %express
						 lager:debug("Raw EID ~p",[EID0]),
						 {EID,Tasks}=case binary:split(EID0,[<<":">>]) of
										[A,BTsk] ->
											 Tsk1=lists:filtermap(
													fun(BTask) ->
															L=binary_to_list(BTask),
															try 
																{true,list_to_existing_atom(L)}
															catch
																_:_ -> false
															end
													end,
													binary:split(BTsk,[<<";">>],[global])),
											 {A,Tsk1};
										 _ -> {EID0,[agg_distance]}
									end,
						 lager:debug("EID ~p, ~p",[EID,Tasks]),
						 case supervisor:start_child(aggregator_sup,[EID,Tasks]) of
							 {ok, Pid} -> lager:debug("Data aggregator ~p runned ~p",[EID, Pid]),
										  true;
							 {error, Err} -> lager:error("Can't run data aggregator: ~p",[Err]),
											 {error, Err}
						 end
						 %try mochijson2:decode(ExpressJSON) of
						%	 {struct,List} when is_list(List) ->
						%		 Key=mng:proplisttom(List),
						%		 Tasks=[agg_distance,agg_distance2],
						%		 case supervisor:start_child(aggregator_sup,[Key,Tasks]) of
						%			 {ok, Pid} -> lager:debug("Data aggregator ~p runned ~p",[Key, Pid]),
						%						  true;
						%			 {error, Err} -> lager:error("Can't run data aggregator: ~p",[Err]),
						%							 error
						%		 end;
						%	 _Any -> 
						%		 lager:error("Can't parse source ~p",[ExpressJSON]),
						%		 error
						% catch
						%	 error:Err ->
						%		 lager:error("Can't parse source ~p",[Err]),
						%		 error
						 %end
				 end,



			   %L: false - no more tasks, true - ok, error 
			   case L of 
				   true -> 
					   State#state{timer=erlang:send_after(1,self(),run_queue)};
%						self() ! run_queue;
				   false -> 
					   State#state{timer=erlang:send_after(10000,self(),run_queue)};
				   error -> 
					   lager:error("Error ~p",[L]),
					   State#state{timer=erlang:send_after(30000,self(),run_queue)};
				   {error,_} -> 
					   lager:error("Error ~p",[L]),
					   State#state{timer=erlang:send_after(30000,self(),run_queue)}
			   end
	   end,
    {noreply, S2};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(run_queue, State) ->
	handle_cast(run_queue,State);

handle_info({message,_Chan,_Payload,SrcPid}, State) ->
	%lager:info("Message ~p",[Payload]),
	eredis_sub:ack_message(SrcPid),
	{noreply, State};
	%handle_cast(run_queue,State);

handle_info({subscribed,_Chan,SrcPid}, State) ->
	eredis_sub:ack_message(SrcPid),
	{noreply, State};

handle_info(Info, State) ->
	lager:info("Info ~p",[Info]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
