%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2015, UENISHI Kota
%%% @doc

%% Perfectly serialized queue. Still unknown under network partition;
%% TODO: replace/3 is not yet implemented.

%%% @end
%%% Created :  3 Jul 2015 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(gen_paxos_serialized_data).

-behaviour(gen_server).

%% API: for now, all API are non-blocking
-export([start_link/4,
         get_ballot_manager/1,
         boot/2,
         call/2,
         tap/2,
         stop/1]).

-include_lib("eunit/include/eunit.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-callback handle_op(Op::term(), Epoch::pos_integer(), State::term()) ->
    {Reply::term(), State::term()}.
-callback handle_tap(Op::term(), Epoch::pos_integer(), State::term()) ->
    Reply::term().

-define(SERVER, ?MODULE).

-record(state, {
          name :: binary(),
          i_base = 0 :: non_neg_integer(),
          ballot_manager :: pid(),
          %% Last epoch decided
          epoch = 0 :: non_neg_integer(),
          %% First epoch not being used for ballot
          next_epoch = 1 :: non_neg_integer(),
          module :: module(),
          state :: term(),
          decided_requests = orddict:new() :: orddict:orddict(),
          pending_requests = orddict:new() :: orddict:orddict()
         }).

%% requests ----> decided (Epoch+1=NextEpoch) --> push/pop --> reply
%%    +--- decided (Epoch+1 != NextEpoch) ---> pending?

%%%===================================================================
%%% API
%%%===================================================================

start_link(Signature, Base, Module, InitState) ->
    Argv = [Signature, Base, Module, InitState],
    gen_server:start_link(?MODULE, Argv, []).

boot(Pid, ConsensuSet) ->
    gen_server:call(Pid, {boot, ConsensuSet}).

get_ballot_manager(Pid) ->
    gen_server:call(Pid, get_ballot_manager).

call(Pid, Op) ->
    gen_server:call(Pid, {call, Op}).

tap(Pid, Op) ->
    gen_server:call(Pid, {tap, Op}).

stop(Pid) ->
    gen_server:call(Pid, stop).
    
%% replace(_, _) ->
%%     ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Name, Base, Module, InitState]) ->
    {ok, Pid} = gen_paxos_ballot_manager:start_link(Name, Base),
    {ok, #state{name=Name,
                ballot_manager=Pid,
                i_base=Base,
                module=Module,
                state=InitState}}.

%% @private
handle_call({boot, ConsensusSet}, _From,
            #state{ballot_manager=Manager} = State) ->
    ok = gen_paxos_ballot_manager:boot(Manager, ConsensusSet),
    {reply, ok, State};
handle_call(get_ballot_manager, _From,
           #state{ballot_manager=Manager} = State) ->
    {reply, {ok, Manager}, State};
handle_call(stop, _From,
           #state{ballot_manager=Manager} = State) ->
    ok = gen_paxos_ballot_manager:stop(Manager),
    {stop, normal, ok, State};

handle_call({call, Op}, From,
            #state{next_epoch=Epoch, ballot_manager=Manager,
                   pending_requests=Reqs0} = State) ->
    Ref = erlang:make_ref(),
    case gen_paxos_ballot_manager:ballot(Manager, Epoch, {Ref, Op}) of
        {ok, started} ->
            Reqs = orddict:append(Epoch, {Ref, Op, From}, Reqs0),
            {noreply, State#state{next_epoch=Epoch+1,
                                  pending_requests=Reqs}};
        {error, {ballot_finished, _F}} ->
            %% ?debugVal({Op, _F}),
            {reply, {error, retry}, State#state{next_epoch=Epoch+1}};
        {error, _} = E ->
            ?debugVal({unexpected, E}),
            {reply, E, State#state{next_epoch=Epoch+1}}
    end;

handle_call({tap, Op}, _From,
            #state{epoch=Epoch, module=Mod, state=SubState} = State) ->
    Reply = Mod:handle_tap(Op, Epoch, SubState),
    {reply, Reply, State}.

%% @private
handle_cast(_, State) ->
    {noreply, State}.

%% @private
handle_info({decided, {_Name, Epoch, {Ref0, Op}}},
            #state{
               decided_requests=DecidedReqs0,
               pending_requests=PendingReqs0,
               epoch=Epoch0,
               module=Mod,
               state=SubState} = State) ->
    {Value0, PendingReqs} = orddict_pop(Epoch, PendingReqs0),
    Value = case Value0 of
                undefined ->
                    {op, Op, undefined};
                {Ref0, Op, From} -> {op, Op, From};
                    %% I am proposer; can execute the operation
                {Ref, Op1, From} ->
                    %% The other proposer have won the ballot; can't
                    %% execute the operation.
                    ?debugVal({conflict, {Ref0, Op}, {Ref, Op1}}),
                    {conflict, Op, From} %% Conflict
            end,
    %% ?debugVal({Epoch, Op}),
    DecidedReqs = orddict:store(Epoch, Value, DecidedReqs0),
    {NewEpoch, SubState1, DecidedReqs1} =
        reply_pending_requests(Epoch0+1, SubState, DecidedReqs, Mod),
    {noreply, State#state{
                epoch=NewEpoch,
                state=SubState1,
                pending_requests=PendingReqs,
                decided_requests=DecidedReqs1}}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

orddict_pop(Key, Orddict) ->
    orddict_pop(Key, Orddict, []).

orddict_pop(_Key, [], InvOrddict) -> 
    {undefined, orddict:from_list(InvOrddict)};
orddict_pop(Key, [{Key, [Value]}|L], Orddict) ->
    {Value, rev_append(Orddict, L)};
orddict_pop(Key, [T|L], Orddict) ->
    orddict_pop(Key, L, [T|Orddict]).

rev_append(L, R) ->
    lists:foldl(fun(E, Acc) -> [E|Acc] end, R, L).
                            
reply_pending_requests(Epoch, SubState, [], _) ->
    {Epoch-1, SubState, []};
reply_pending_requests(Epoch, SubState,
                       [{Epoch, {Type, Op, From}}|Reqs0],
                       Mod) ->
    {Reply0, SubState1} = Mod:handle_op(Op, Epoch, SubState),
    case From of
        undefined -> noop;
        _ ->
            Reply = case Type of
                        op -> Reply0;
                        conflict -> {error, conflict}
                    end,
            gen_server:reply(From, Reply)
    end,
    reply_pending_requests(Epoch+1, SubState1, Reqs0, Mod);
reply_pending_requests(Epoch, SubState,
                       [{Epoch, [{Type, Op, From}|L]}|Reqs0],
                       Mod) ->
    ?debugVal(L),
    reply_pending_requests(Epoch, SubState,
                           [{Epoch, {Type, Op, From}}|Reqs0],
                           Mod);
reply_pending_requests(Epoch, SubState, Reqs, _) ->
    {Epoch-1, SubState, Reqs}.
    
    
