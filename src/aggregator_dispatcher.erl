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
				max_worker=5,
				max_express=5,
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
handle_cast(run_queue, State) ->
	case State#state.timer of 
		undefined -> ok;
		_ -> erlang:cancel_timer(State#state.timer)
	end,
	Allow=case proplists:get_value(workers,supervisor:count_children(aggregator_sup)) of
		undefined -> 
			lager:error("Can't get worker count"),
			false;
		M when is_integer(M) ->
				  State#state.max_worker > M 
		  end,
	lager:debug("Allow run queue ~p",[Allow]),
	S2=case Allow of
		false -> 
			State#state{timer=erlang:send_after(5000,self(),run_queue)};
		true -> 
			Fun=fun(Worker) -> 
						{ok, J} = eredis:q(Worker, [ "rpop", "process:devicedata" ]),
						J
				end,
			BJSON=poolboy:transaction(ga_redis, Fun),
			L=case BJSON of 
				undefined -> false;
				_ -> 
					try mochijson2:decode(BJSON) of
						{struct,List} when is_list(List) ->
							Key=mng:proplisttom(List),
							Tasks=[agg_distance,agg_distance2],
							case supervisor:start_child(aggregator_sup,[Key,Tasks]) of
								{ok, Pid} -> lager:debug("Data aggregator ~p runned ~p",[Key, Pid]),
											 true;
								{error, Err} -> lager:error("Can't run data aggregator: ~p",[Err]),
												error
							end;
						_Any -> 
							lager:error("Can't parse source ~p",[BJSON]),
							error
					catch
						error:Err ->
							lager:error("Can't parse source ~p",[Err]),
							error
					end
			end,
			%L: false - no more tasks, true - ok, error 
			case L of 
				true -> 
					gen_server:cast(self(),run_queue),
					State;
				false -> 
					State#state{timer=erlang:send_after(10000,self(),run_queue)};
				error -> 
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
	gen_server:cast(self(),run_queue),
	{noreply, State};

handle_info({message,_Chan,_Payload,SrcPid}, State) ->
	%lager:info("Message ~p",[Payload]),
	eredis_sub:ack_message(SrcPid),
	gen_server:cast(self(),run_queue),
	{noreply, State};

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
