%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2015, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created : 28 Jun 2015 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(gen_paxos_proposer).

-behaviour(gen_fsm).

%% API
-export([start_link/4, stop/1]).

%% gen_fsm callbacks
-export([init/1,
         preparing/2, preparing/3,
         proposing/2, proposing/3,
         finished/2, finished/3,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {
          acceptors = [] :: [pid()],
          value :: term(),
          done_fun :: fun(),
          initial_n :: non_neg_integer(),
          n :: non_neg_integer(),
          prepare_acks = [] :: list(),
          propose_acks = [] :: list()
         }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Nodes, ProposalNumber, Value, DoneFun) when ProposalNumber > 0 ->
    gen_fsm:start_link(?MODULE,
                       [Nodes, ProposalNumber, Value, DoneFun], []).

stop(Pid) ->
    try
        gen_fsm:sync_send_all_state_event(Pid, stop)
    catch exit:{noproc, _FMA} ->
            ok
    end.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([Acceptors, ProposalNumber, Value, DoneFun]) ->
    %% Acceptors = [{gen_paxos_acceptor, N}||N<-Nodes],
    [gen_paxos_acceptor:prepare(Acceptor, ProposalNumber)
     ||Acceptor <- Acceptors],
    {ok, preparing,
     #state{
        acceptors = Acceptors,
        initial_n = ProposalNumber,
        n = ProposalNumber,
        value = Value,
        done_fun = DoneFun
       }}.

%% @private
preparing({prepare_ack, Pid, N},
          #state{prepare_acks=Acks, acceptors=Acceptors,
                 value=Value, n=N} = State) ->
    case lists:member(Pid, Acks) of
        true ->
            {next_state, preparing, State};
        false ->
            NextStateName =
                case is_quorum(Acks, Acceptors) of
                    true -> %% Quorum
                        [gen_paxos_acceptor:accept(Acceptor, Value, N)
                         ||Acceptor <- Acceptors],
                        proposing;
                    false ->
                        preparing
                end,
            {next_state, NextStateName,
             State#state{prepare_acks=[Pid|Acks]}}
    end;
preparing({stale, _Pid, Nrecv},
          #state{n=N} = State) when Nrecv > N ->
    %% Acceptor has already accepted other early proposal, Giving up
    {stop, normal, State#state{n=Nrecv}};
preparing({stale, _Pid, Nrecv},
          #state{n=N} = State) when Nrecv < N ->
    %% Acceptor has already accepted other early proposal, Giving up
    {stop, normal, State}.

proposing({accepted, Pid, N},
          #state{propose_acks=Acks, acceptors=Acceptors,
                 value = Value, done_fun=DoneFun,
                 n=N} = State) ->
    case lists:member(Pid, Acks) of
        true ->
            {next_state, proposing, State};
        false ->
            NextStateName =
                case is_quorum(Acks, Acceptors) of
                    true -> %% Quorum
                        DoneFun({ok, Value}),
                        finished;
                    false ->
                        proposing
                end,
            {next_state, NextStateName,
             State#state{propose_acks=[Pid|Acks]}}
    end;
proposing({prepare_ack, Pid, N},
          #state{prepare_acks=Acks, n=N} = State) ->
    case lists:member(Pid, Acks) of
        true ->
            {next_state, proposing, State};
        false ->
            {next_state, proposing, State#state{prepare_acks=[Pid|Acks]}}
    end;
proposing({stale, _Pid, Nrecv},
          #state{n=N} = State) when Nrecv > N ->
     %% Acceptor has already accepted other larger-N proposal, Giving up
     {stop, normal, State}.

finished({accepted, Pid, N},
          #state{propose_acks=Acks, n=N} = State) ->
    NewAcks = case lists:member(Pid, Acks) of
                  true -> Acks;
                  false -> [Pid|Acks]
              end,
    {next_state, finished, State#state{propose_acks=NewAcks}};
            
finished(_Event, State) ->
    {next_state, finished, State}.

%%--------------------------------------------------------------------
%% @private
preparing(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, preparing, State}.
proposing(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, proposing, State}.
finished(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, finished, State}.

%% @private
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @private
handle_sync_event(stop, _From, _StateName, State) ->
    Reply = ok,
    {stop, normal, Reply, State}.

%% @private
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @private
terminate(_Reason, _StateName, _State) ->
    ok.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

is_quorum(Acks, Set) when is_list(Acks) andalso is_list(Set) ->
    2 * (length(Acks) + 1) > length(Set).
