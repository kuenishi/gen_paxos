-module(gen_paxos_learner).

-behaviour(gen_fsm).

%% API
-export([start_link/2, start_link/3,
         add_acceptor/2,
         accepted/2, stop/1]).

%% gen_fsm callbacks
-export([init/1,
         waiting/2, waiting/3,
         learned/2, learned/3,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {acceptors = [] :: [pid()], %% Known acceptors, not all
                n_val :: pos_integer(),
                values = [] :: [term()],
                value :: term(),
                caller :: pid()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pid(), pid()) -> {ok, pid()} | {error, term()}.
start_link(Acceptors, Caller) ->
    start_link(Acceptors, Caller, length(Acceptors)).

start_link(Acceptors, Caller, NVal) when length(Acceptors) =< NVal ->
    gen_fsm:start_link(?MODULE, [Acceptors, Caller, NVal], []).

add_acceptor(Pid, APid) ->
    gen_fsm:send_all_state_event(Pid, {add_acceptor, APid}).

stop(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stop).

%% Called by Acceptor
accepted(Pid, Value) ->
    gen_fsm:send_event(Pid, {accepted, Value}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([Acceptors, Caller, NVal]) ->
    [gen_paxos_acceptor:poke(Acceptor)||Acceptor <- Acceptors],
    %% TODO monitor all acceptors to know consensus broken or not
    {ok, waiting, #state{acceptors=Acceptors,
                         caller=Caller,
                         n_val=NVal}}.

waiting({accepted, Value},
        State = #state{values=Vs, caller=Caller,
                       n_val=NVal}) ->
    
    NewValues = [Value|Vs],
    case majority_value(NewValues) of
        %% {0, noop} -> %% does not reach here ...
        {Count, Value} when Count * 2 > NVal ->
            %% Majority here;
            %% Should have notification system here, like logging
            try
                Caller ! {decided, self(), Value}
            after
                ok
            end,
            {next_state, learned, State#state{values=NewValues,
                                              value=Value}};
        {_Count, _Value} ->
            {next_state, waiting, State#state{values=NewValues}}
    end.

learned({accepted, Value}, State = #state{values=Vs}) ->
    {next_state, learned, State#state{values=[Value|Vs]}}.

waiting(_Event, _From, State) ->
    Reply = wat,
    {reply, Reply, waiting, State}.

learned(_Event, _From, State) ->
    Reply = 'wat?',
    {reply, Reply, learned, State}.

%% @private
%% @doc
handle_event({add_acceptor, APid}, StateName,
             #state{acceptors=As, n_val=N} = State) ->
    case length(As) of
        N ->
            {stop, {error, {extra_acceptor, APid}}, State};
        L when L < N ->
            gen_paxos_acceptor:poke(APid),
            {next_state, StateName,
             State#state{acceptors=[APid|As]}}
    end.

%% @private
%% @doc
handle_sync_event(_Event, _From, _StateName, State) ->
    {stop, normal, ok, State}.

%% @private
%% @doc
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @private
%% @doc
terminate(_Reason, _StateName, _State) ->
    ok.

%% @private
%% @doc
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

majority_value([]) -> {0, noop};
majority_value(Values) ->
    F = fun(V, Counts) ->
                C = proplists:get_value(V, Counts, 0),
                [{V, C+1}|proplists:delete(V, Counts)]
        end,
    Cs0 = lists:foldl(F, [], Values),
    Cs1 = lists:sort([{Count, Value} || {Value, Count} <- Cs0 ]),
    lists:last(Cs1).
    
                    
    

