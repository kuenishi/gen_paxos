%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2015, UENISHI Kota
%%% @doc

%% There are a lot more to design, one ballot corresponds to one value
%% pushed into the queue, each represented by sequence number. Another
%% ballot is for dequeueing. How do we ensure the concurrency control?
%% One queue per one process.

%%% @end
%%% Created :  3 Jul 2015 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(gen_paxos_ballot_manager).

-behaviour(gen_server).

%% API: for now, all API are non-blocking
-export([start_link/2, start_link/3,
         boot/2, trim/2, status/1,
         peek/2, ballot/3,
         replace/3,
         stop/1]).

-include_lib("eunit/include/eunit.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_BALLOT_BUFFER_SIZE, 8).

-record(ballot, {
          epoch = 0 :: non_neg_integer(),
          acceptor_set = [] :: [pid()],
          acceptor :: pid(),
          proposer :: pid(),
          learner :: pid(),
          value :: term()
         }).

-record(state, {
          name :: binary(),
          i_base = 0 :: non_neg_integer(),
          consensus_set = [] :: [pid()],
          controlling_process :: pid(),

          %% ballot store
          oldest_live_ballot = 0 :: non_neg_integer(), %% the last one not trimmed
          latest_ballot_epoch = 0 :: non_neg_integer(), %% latest unused
          largest_ballot_epoch = 0 :: non_neg_integer(), %% largest prepared ballot in buffer
          ballot_buffer = ets:new(?MODULE, [ordered_set, private]) :: ets:tid()
         }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Signature, Base) ->
    start_link(Signature, Base, 0).

start_link(Signature, Base, InitEpoch)
    when is_integer(Base), Base > 0, is_integer(InitEpoch), InitEpoch >= 0 ->
    Controller = self(),
    gen_server:start_link(?MODULE,
                          [Signature, Base, InitEpoch, Controller],
                          []).

%% @doc You cannot boot your consensus set unless all peers are up,
%% ready and visible.
boot(Pid, ConsensusSet) when is_list(ConsensusSet) ->
    gen_server:call(Pid, {boot, ConsensusSet}).

%% @doc trim old ballot consensus. After the trim, the peer is unable
%% to see the ballot result where Epoch < Epoch0
trim(Pid, Epoch0) when is_integer(Epoch0) andalso 0 < Epoch0 ->
    gen_server:call(Pid, {trim, Epoch0}).

%% @doc retrieve the status, like epoch number or so.
status(Pid) ->
    gen_server:call(Pid, status).

%% @doc see the result of ballot with Epoch.
peek(Pid, Epoch)
  when is_integer(Epoch) andalso Epoch >= 0 ->
    gen_server:call(Pid, {peek, Epoch}).

%% @doc ask for a ballot with specified ballot.
ballot(Pid, Epoch, Value)
  when is_integer(Epoch) andalso Epoch >= 0 ->
    gen_server:call(Pid, {ballot, Epoch, Value}).

%% @doc replace a peer, pass new and old ballot manager pid The only
%% assumption for this function is, that the old one is DEAD FOR
%% SURE. The use case for this is just an operation to replace a
%% failed node.
replace(Pid, Old, Rookie = {_RookiePid, RookieEpoch})
  when is_integer(RookieEpoch) andalso RookieEpoch >= 0 ->
    gen_server:call(Pid, {replace, Old, Rookie}).

stop(Pid) ->
    gen_server:call(Pid, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
init([Name, Base, Epoch, Controller]) ->
    State = #state{name=Name, i_base=Base,
                   controlling_process = Controller},
    #state{ballot_buffer = Buffer} = State,
    Largest = expand_ballot_buffer(Buffer, Epoch, undefined),
    {ok, State#state{oldest_live_ballot=Epoch,
                     latest_ballot_epoch=Epoch,
                     largest_ballot_epoch=Largest}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
handle_call({boot, ConsensusSet}, _From,
            #state{name=Name,
                   i_base=Base,
                   ballot_buffer=Buffer,
                   latest_ballot_epoch=Latest} = State) ->
    StartLearnerFun =
        fun({E, Ballot = #ballot{learner=undefined,
                                     acceptor_set=As}}, Acc0) ->
                LPid = maybe_start_learner(undefined, As, length(ConsensusSet)),
                [{E, Ballot#ballot{learner=LPid}}|Acc0];
           (_, Acc) ->
                Acc
        end,
    NewBallots = ets:foldl(StartLearnerFun, [], Buffer),
    true = ets:insert(Buffer, NewBallots),
    broadcast_exchange(ConsensusSet, Name, Base, Buffer, Latest),
    NewState = State#state{consensus_set=ConsensusSet},
    {reply, ok, NewState};

handle_call({ballot, Epoch, Value} = Msg, From,
            #state{ballot_buffer=Buffer,
                   consensus_set=CSet,
                   name=Name,
                   oldest_live_ballot=Oldest,
                   largest_ballot_epoch=Largest,
                   i_base=Base} = State)
  when Epoch =< Largest ->
    case ets:lookup(Buffer, Epoch) of
        [{Epoch, Ballot = #ballot{value=OldValue}}] ->
            %% TBD: Start a ballot or refer old value
                case OldValue of
                    undefined ->
                        Reply = maybe_start_ballot(Buffer, Ballot, Base, Value),
                        {reply, Reply, State};
                    OldValue ->
                        %% Ballot already finished
                        {reply, {error, {ballot_finished, OldValue}}, State}
                end;       
        [] when Epoch =< Oldest ->
            {reply, {error, too_old_ballot}, State};
        [] ->
            NewLargest = expand_ballot_buffer(Buffer, Largest + 1, length(CSet)),
            broadcast_exchange(CSet, Name, Base, Buffer, Largest + 1),
            NewState = State#state{largest_ballot_epoch=NewLargest},
            %% expand ballot and try again
            handle_call(Msg, From, NewState);
        Error ->
            ?debugVal(Error),
            error(Error)
    end;
handle_call({ballot, Epoch, _Value} = Msg, From,
            #state{ballot_buffer=Buffer,
                   consensus_set=CSet,
                   name=Name,
                   i_base=Base,
                   largest_ballot_epoch=Largest} = State)
  when Largest < Epoch ->
    NewLargest = expand_ballot_buffer(Buffer, Largest + 1, length(CSet)),
    broadcast_exchange(CSet, Name, Base, Buffer, Largest + 1),
    handle_call(Msg, From, State#state{largest_ballot_epoch=NewLargest});

handle_call({peek, Epoch}, _From,
            #state{ballot_buffer=Buffer} = State) ->
    case ets:lookup(Buffer, Epoch) of
        [{Epoch, _Ballot = #ballot{value=undefined}}] ->
            %% Ballot not just started, or running
            ?debugVal(_Ballot),
            {reply, {error, notfound}, State};
        [{Epoch, _Ballot = #ballot{value=Value}}] ->
            {reply, {ok, Value}, State};
        [] ->
            ?debugHere,
            {reply, {error, notfound}, State};
        Error ->
            error(Error)
    end;

handle_call({trim, Epoch0}, _From,
            #state{ballot_buffer=Buffer} = State) ->
    %% delete old ballots here, after stopping all Pids
    MatchHead = {'$0', '$1'},
    MatchGuard = [{ '<', '$0', Epoch0}],
    ToBeDeleted = ets:select(Buffer,
                             [{MatchHead, MatchGuard, ['$_']}]),
    [begin
         ok = gen_paxos_acceptor:stop(APid),
         ok = gen_paxos_learner:stop(LPid),
         case PPid of
             undefined -> noop;
             PPid when is_pid(PPid) ->
                 ok = gen_paxos_proposer:stop(PPid)
         end
     end || #ballot{acceptor=APid,
                    learner=LPid,
                    proposer=PPid} <- ToBeDeleted ],
    Deleted = ets:select_delete(Buffer,
                                [{MatchHead, MatchGuard, [true]}]),
    {reply, {ok, Deleted}, State#state{oldest_live_ballot=Epoch0}};

handle_call({replace, Old, {Rookie, RookieEpoch}}, _From,
            #state{consensus_set=CSet,
                   largest_ballot_epoch=Largest} = State)
  when Largest < RookieEpoch ->
    %% You cannot change ballots in current buffer - just leave them
    %% and wait for trim
    NewCSet = [Rookie|lists:delete(Old, CSet)],
    ok = gen_paxos_ballot_manager:boot(Rookie, NewCSet),
    {reply, ok, State#state{consensus_set=NewCSet}};

handle_call({replace, _Old, _}, _From, State) ->
    {reply, {error, too_small_rookie_epoch}, State};

handle_call(status, _, State) -> %% TBD
    {reply, {ok, State}, State};

handle_call(stop, _, State) -> %% TBD
    {stop, normal, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
handle_cast({exchange, Name, Base, EAs} = Msg,
            #state{name=Name,
                   i_base=Base0,
                   consensus_set=CSet,
                   largest_ballot_epoch=Largest0,
                   ballot_buffer=Buffer} = State)
  when Base =/= Base0 ->
    %% Just get the largest epoch of all EAs
    case lists:foldl(fun({E,_}, Max) -> erlang:max(E, Max) end,
                     Largest0,
                     EAs) of
        L when L > Largest0 ->
            Largest = expand_ballot_buffer(Buffer, Largest0 + 1,
                                           L, length(CSet)),
            broadcast_exchange(CSet, Name, Base0, Buffer, Largest0 + 1),
            handle_cast(Msg, State#state{largest_ballot_epoch=Largest});
        _L ->
            add_acceptors_to_buffer(Buffer, length(CSet), EAs),
            {noreply, State}
    end;

handle_cast({exchange, _, _, _} = Exchange,
            State) ->
    ?debugHere,
    {stop, wrong_node_in_cluster, {Exchange, State}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
handle_info({decided, LearnerPid, {Epoch, Value}} = Msg,
            #state{ballot_buffer=Buffer,
                   controlling_process=Controller,
                   consensus_set=CSet,
                   name=Name,
                   i_base=Base,
                   oldest_live_ballot=Oldest,
                   largest_ballot_epoch=Largest0} = State) -> %% From learner
    case ets:lookup(Buffer, Epoch) of
        [{Epoch, Ballot = #ballot{learner=LearnerPid}}] ->
            %% ?debugVal({LearnerPid, Epoch, Value}),
            NewBallot = Ballot#ballot{value=Value},
            true = ets:insert(Buffer, {Epoch, NewBallot}),
            
            Controller ! {decided, {Name, Epoch, Value}},
            %% If no (few) buffer left, expand buffer
            Largest = case Largest0 - Epoch < ?DEFAULT_BALLOT_BUFFER_SIZE of
                          true -> %% Expand
                              L = expand_ballot_buffer(Buffer, Largest0 + 1, length(CSet)),
                              broadcast_exchange(CSet, Name, Base, Buffer, Largest0 + 1),
                              L;
                          false ->
                              Largest0
                      end,
            {noreply, State#state{latest_ballot_epoch=Epoch,
                                  largest_ballot_epoch=Largest}};
        [] when Epoch < Oldest ->
            ?debugFmt("Too old: ~p, while oldest = ~p", [Msg, Oldest]),
            {noreply, State};
        [] ->
            %% TBD: Value decided but no ballot ready here, probably
            %% some network separation or slow down of this node
            L0 = expand_ballot_buffer(Buffer, Largest0 + 1, length(CSet)),
            broadcast_exchange(CSet, Name, Base, Buffer, Largest0 + 1),
            NewState = State#state{largest_ballot_epoch=L0},
            handle_info(Msg, NewState)
    end;
 handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc sends exchange message and reply to all of them
broadcast_exchange(CSet, Name, Base, Buffer, StartEpoch) ->
    Fun = fun({E, #ballot{epoch=E, acceptor=Pid}} = _, Acc0)
             when StartEpoch =< E ->
                  [{E, Pid}|Acc0];
             (_, Acc) ->
                  Acc
          end,
    EAs = ets:foldl(Fun, [], Buffer),
    Self = self(),
    [gen_server:cast(Pid, {exchange, Name, Base, EAs})
     || Pid <- CSet, Pid =/= Self].

add_acceptors_to_buffer(_, _N, []) -> ok;
add_acceptors_to_buffer(Buffer, N, [{Epoch, Acceptor}|EAs]) ->
    case ets:lookup(Buffer, Epoch) of
        [{Epoch, Ballot = #ballot{acceptor_set=As0,
                                  learner=Learner0}}] ->
            As = case lists:member(Acceptor, As0) of
                     true -> As0;
                     false -> [Acceptor|As0]
                 end,
            Learner = maybe_start_learner(Learner0, As, N),
            %% TODO: check Acceptor is not yet in As0
            NewBallot = Ballot#ballot{acceptor_set=As,
                                      learner=Learner},
            true = ets:insert(Buffer, {Epoch, NewBallot}),
            add_acceptors_to_buffer(Buffer, N, EAs);
        [] ->
            %% The buffer should be expanded before here
            %% throw({epoch_notfound, Epoch});
            {error, Epoch};
        _ ->
            %% Never happens, as Buffer is ordered_set
            throw({bug, ?FILE, ?LINE})
    end.

maybe_start_learner(Learner0, _, 0) ->
    Learner0;
maybe_start_learner(Learner0, As, _N) when is_pid(Learner0) ->
    gen_paxos_learner:add_acceptor(Learner0, hd(As)),
    Learner0;
maybe_start_learner(_, As, N) ->
    {ok, Pid} = gen_paxos_learner:start_link(As, self(), N),
    Pid.

maybe_start_ballot(Buffer,
                   Ballot0 = #ballot{epoch=Epoch,
                                     acceptor_set=ASet,
                                     proposer=undefined},
                   Base,
                   Value) ->
    DoneFun = fun(_Result) ->
                      %% ?debugFmt("decided ~p as ~p (proposed: ~p)",
                      %%           [Epoch, Result, Value])
                      noop
              end,
    {ok, P} = gen_paxos_proposer:start_link(ASet, Base, {Epoch, Value}, DoneFun),
    Ballot = Ballot0#ballot{proposer=P},
    true = ets:insert(Buffer, {Epoch, Ballot}),
    {ok, started}.
    
expand_ballot_buffer(Buffer, StartEpoch, NVal) ->
    EndEpoch = StartEpoch + ?DEFAULT_BALLOT_BUFFER_SIZE * 2,
    expand_ballot_buffer(Buffer, StartEpoch, EndEpoch, NVal).

expand_ballot_buffer(Buffer, StartEpoch, EndEpoch, NVal) ->
    Ballots = [begin
                   {ok, APid} = gen_paxos_acceptor:start_link(),
                   LPid = case NVal of
                              undefined -> undefined;
                              NVal when is_integer(NVal) ->
                                  maybe_start_learner(undefined, [APid], NVal)
                          end,
                   {S, #ballot{epoch = S,
                               acceptor_set = [APid],
                               acceptor = APid,
                               learner=LPid}}
               end || S <- lists:seq(StartEpoch, EndEpoch)],
    true = ets:insert_new(Buffer, Ballots),
    %% ?debugFmt("Ballot expanded here: ~p => ~p", [StartEpoch, EndEpoch]),
    EndEpoch.

