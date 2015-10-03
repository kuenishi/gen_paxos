%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2015, UENISHI Kota
%%% @doc

%%%   Leader election example using gen_paxos proposer, acceptor and
%%%   learner. Basic idea is to have them all as child process in the
%%%   tree.
%%%   TODO: healthy and cool replace/3 implementation

%%% @end
%%% Created :  2 Jul 2015 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(gen_paxos_leader).

-behaviour(gen_fsm).

%% API
-export([start_link/3,
         get_ballot_manager/1,
         status/1, boot/2, stop/1, leader/1,
         replace/3]).

%% gen_fsm callbacks
-export([init/1,
         preparing/2, preparing/3,
         electing/2, electing/3,
         leader/2, leader/3,
         follower/2, follower/3,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).
-define(DEFAULT_LEASE_TIME_SEC, 5).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
          ballot_manager :: pid(),
          lease_time_sec = ?DEFAULT_LEASE_TIME_SEC  :: non_neg_integer(),
          lease_timer :: undefined | reference(),
          epoch = 0 :: non_neg_integer(), %% Current epoch
          leader = undefined :: undefined | pid(), %% Current leader
          name :: term(),
          controlling_process :: undefined | pid()}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
-spec start_link(term(), pos_integer(), proplists:proplist()) ->
                        {ok, pid()} | {error, term()}.
start_link(Signature, Base, Options) ->
    gen_fsm:start_link(?MODULE, [Signature, Base, Options], []).

get_ballot_manager(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, get_ballot_manager).

-spec status(pid()) -> {atom(), #state{}}.
status(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, status).

-spec boot(pid(), [pid()]) -> ok.
boot(Pid, ConsensusSet) ->
    gen_fsm:sync_send_event(Pid, {boot, ConsensusSet}).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stop).

-spec leader(pid()) -> {pos_integer(), pid()}.
leader(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, leader).

-spec replace(pid(), pid(), pid()) -> ok.
replace(_Pid, _OldPid, _NewPid) ->
    ok.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @private
init([Signature, Base, Options]) ->
    LeaseTime = proplists:get_value(lease_time_sec, Options,
                                    ?DEFAULT_LEASE_TIME_SEC),
    State = #state{lease_time_sec = LeaseTime},
    {ok, Pid} = gen_paxos_ballot_manager:start_link(Signature, Base),
    Controller = proplists:get_value(controlling_process, Options),
    {ok, preparing, State#state{controlling_process=Controller,
                                ballot_manager=Pid}}.

%%--------------------------------------------------------------------
%% @private
preparing({elected, {Epoch, NewLeader}},
          #state{epoch=OldEpoch} = State) when OldEpoch < Epoch ->
    new_era(State, Epoch, NewLeader).

electing({elected, {Epoch, NewLeader}},
         #state{epoch=Epoch} = State) ->
    new_era(State, Epoch+1, NewLeader);
electing({elected, {Epoch, NewLeader}},
          #state{epoch=OldEpoch} = State) when OldEpoch < Epoch ->
    new_era(State, Epoch, NewLeader).

leader({elected, {Epoch, NewLeader}},
          #state{epoch=OldEpoch} = State) when OldEpoch < Epoch ->
    new_era(State, Epoch, NewLeader).

follower({elected, {Epoch, NewLeader}},
         #state{epoch=OldEpoch} = State) when OldEpoch < Epoch ->
    new_era(State, Epoch, NewLeader).

%%--------------------------------------------------------------------
%% @private
%% @doc
preparing({boot, ConsensusSet}, _From,
          #state{ballot_manager=Manager,
                 epoch=Epoch} = State) ->

    ok = gen_paxos_ballot_manager:boot(Manager, ConsensusSet),
    timer:sleep(7),
    {ok, _} = start_election(Manager, Epoch),
    {reply, ok, electing, State}.

electing(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, electing, State}.

leader(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, leader, State}.
follower(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, follower, State}.

%% @private
%% @doc
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @private
%% @doc
handle_sync_event(get_ballot_manager, _From, StateName,
                  #state{ballot_manager=Manager} = State) ->
    {reply, {ok, Manager}, StateName, State};

handle_sync_event(leader, _From, StateName,
                  #state{leader=Leader, epoch=Epoch} = State) ->
    Reply = case StateName of
                leader -> {ok, {Epoch, Leader}};
                follower -> {ok, {Epoch, Leader}};
                electing-> {ok, {Epoch, undefined}};
                _ -> {error, not_booted}
            end,
    {reply, Reply, StateName, State};
handle_sync_event(status, _From, StateName, State) ->
    {reply, {StateName, State}, StateName, State};
handle_sync_event(stop, _From, _StateName, State) ->
    {stop, normal, ok, State}.

%% @private
%% @doc
handle_info({decided, {_Name, Epoch, NewLeader}}, _StateName,
             #state{epoch=Epoch} = State) ->
    new_era(State, Epoch + 1, NewLeader);
handle_info({decided, {_Name, Epoch, NewLeader}}, _StateName,
             #state{epoch=Epoch0} = State) when Epoch > Epoch0 ->
    new_era(State, Epoch, NewLeader);
handle_info({decided, {_Name, Epoch, _NewLeader}}, StateName,
             #state{epoch=Epoch0} = State) when Epoch < Epoch0 ->
    {next_state, StateName, State};

handle_info(lease_end, StateName,
            #state{
               ballot_manager=Manager,
               epoch=Epoch,
               controlling_process=Controller} = State)
  when StateName =:= leader orelse StateName =:= follower->
    case Controller of
        undefined -> noop;
        Pid -> Pid ! {lease_end, Epoch}
    end,
    {ok, started} = start_election(Manager, Epoch),
    {next_state, electing, State}.

%% @private
%% @doc
terminate(_Reason, _StateName, _State) ->
    ok.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_election(Manager, Epoch) ->
    {ok, started} = gen_paxos_ballot_manager:ballot(Manager, Epoch, self()).

new_era(OldState, NewEpoch, NewLeader) ->
    %% ?debugVal({NewLeader, NewEpoch}),
    %% TODO: trim old ballots here
    NewState = start_lease(OldState#state{epoch=NewEpoch,
                                          leader=NewLeader}),
    case OldState#state.controlling_process of
        undefined -> noop;
        Pid -> Pid ! {elected, {NewLeader, NewEpoch}}
    end,
    case self() of
        NewLeader ->
            {next_state, leader, NewState};
        _ ->
            {next_state, follower, NewState}
    end.

start_lease(#state{lease_timer = Timer} = State) when Timer =/= undefined ->
    erlang:cancel_timer(Timer),
    start_lease(State#state{lease_timer=undefined});
start_lease(#state{lease_time_sec=Lease} = State)
  when is_integer(Lease) andalso Lease > 0 ->
    TimerRef = erlang:send_after(Lease * 1000, self(), lease_end),
    State#state{lease_timer=TimerRef}.
    

