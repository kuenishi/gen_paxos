-module(gen_paxos_acceptor).

-behaviour(gen_fsm).

%% API
-export([start_link/0, status/1, stop/1, poke/1,
         prepare/2, accept/3]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

%% States
-export([prepared/2, prepared/3,
         accepted/2, accepted/3]).

-define(SERVER, ?MODULE).

-record(state, {
          %% nodes :: list(),
          n = 0 :: non_neg_integer(),
          proposer :: undefined | pid(),
          value :: undefined | term(),
          learners = [] :: [pid()]
         }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_fsm:start_link(?MODULE, [], []).

status(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, status).

stop(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stop).

poke(Pid) ->
    Self = self(),
    gen_fsm:send_all_state_event(Pid, {poke, Self}).

prepare(Proc, N) ->
    Self = self(),
    gen_fsm:send_event(Proc, {prepare, Self, N}).

accept(Proc, Value, N) ->
    Self = self(),
    gen_fsm:send_event(Proc, {accept, Self, Value, N}).


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([]) ->
%%     State = #state{nodes = Nodes},
    {ok, prepared, #state{}}.

prepared({prepare, FromPid, Nrecv}, #state{n=N} = State)
  when Nrecv > N ->
    gen_fsm:send_event(FromPid, {prepare_ack, self(), Nrecv}),
    {next_state, prepared,
     State#state{proposer=FromPid, n=Nrecv}};
prepared({prepare, FromPid, Nrecv}, #state{n=N} = State)
  when is_integer(Nrecv) andalso N > 0 ->
    gen_fsm:send_event(FromPid, {stale, self(), N}),
    {next_state, prepared, State};

prepared({accept, FromPid, Value, N},
         #state{n=N, learners=L} = State)
  when N > 0 ->
    %% Accepted!
    gen_fsm:send_event(FromPid, {accepted, self(), N}),
    [catch gen_paxos_learner:accepted(LearnerPid, Value) || LearnerPid <- L],
    {next_state, accepted, State#state{value=Value}};
prepared({accept, FromPid, _, Nrecv}, #state{n=N} = State)
  when Nrecv < N ->
    gen_fsm:send_event(FromPid, {stale, self(), N}),
    {next_state, prepared, State}.

accepted(_Event, State) ->
    {next_state, accepted, State}.

%% call handlers
prepared(_Event, _From, State) ->
    {reply, ok, prepared, State}.
accepted(_Event, _From, State) ->
    {reply, ok, accepted, State}.

handle_event({poke, LearnerPid}, StateName,
             #state{learners=L, value=V} = State) ->
    case StateName of
        accepted ->
            gen_paxos_learner:accepted(LearnerPid, V);
        _ ->
            noop
    end,
    {next_state, StateName, State#state{learners=[LearnerPid|L]}};

handle_event(_, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName, State) ->
    Reply = {StateName, State},
    {reply, Reply, StateName, State};
handle_sync_event(stop, _From, StateName, State) ->
    Reply = {StateName, State},
    {stop, normal, Reply, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
