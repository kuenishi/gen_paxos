-module(basic_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

single_acceptor_test() ->
    {ok, Pid} = gen_paxos_acceptor:start_link(),
    Acceptors = [Pid],
    This = self(),
    DoneFun = fun(Value) ->
                      This ! Value
              end,
    {ok, Prop} = gen_paxos_proposer:start_link(Acceptors, 7, boom, DoneFun),
    receive
        V -> ?debugVal(V)
    end,
    ?debugVal(gen_paxos_acceptor:status(Pid)),
    gen_paxos_acceptor:stop(Pid),
    gen_paxos_proposer:stop(Prop),
    ok.

two_acceptors_test() ->
    {ok, P1} = gen_paxos_acceptor:start_link(),
    {ok, P2} = gen_paxos_acceptor:start_link(),
    Acceptors = [P1, P2],
    This = self(),
    DoneFun = fun(Value) ->
                      This ! Value
              end,
    {ok, Pid} = gen_paxos_proposer:start_link(Acceptors, 7, boom, DoneFun),
    receive
        {ok, V} -> ?debugVal(V);
        Other -> ?assert(Other)
    end,
    [begin
         %% ?debugVal(gen_paxos_acceptor:status(P)),
         gen_paxos_acceptor:stop(P)
     end || P <- Acceptors],
    gen_paxos_proposer:stop(Pid).

many_acceptor_test() ->
    Acceptors = [element(2, gen_paxos_acceptor:start_link())
                 || _ <- lists:seq(1, 37)],
    This = self(),
    DoneFun = fun(Value) ->
                      This ! Value
              end,
    {ok, Pid} = gen_paxos_proposer:start_link(Acceptors, 7, spam, DoneFun),
    {ok, Pid2} = gen_paxos_proposer:start_link(Acceptors, 9, ham, DoneFun),
    %% timer:sleep(1),
    %% [?debugVal(gen_paxos_acceptor:status(P))|| P <- Acceptors],
    receive
        {ok, V} -> ?debugVal(V);
        Other -> ?assert(Other)
    end,
    [gen_paxos_acceptor:stop(P) || P <- Acceptors],
    gen_paxos_proposer:stop(Pid),
    gen_paxos_proposer:stop(Pid2).

learner_test() ->
    {ok, P1} = gen_paxos_acceptor:start_link(),
    {ok, P2} = gen_paxos_acceptor:start_link(),
    Acceptors = [P1, P2],
    {ok, L} = gen_paxos_learner:start_link(Acceptors, self()),
    This = self(),
    DoneFun = fun(Value) ->
                      This ! Value
              end,
    {ok, Pid} = gen_paxos_proposer:start_link(Acceptors, 7, boom, DoneFun),
    receive
        {ok, V} ->
            ?debugVal(V),
            receive
                {decided, _, Value} ->  ?assertEqual(V, Value);
                Other -> ?assert(Other)
            end;
        Other ->
            ?assert(Other)
    end,
    [begin
         %% ?debugVal(gen_paxos_acceptor:status(P)),
         gen_paxos_acceptor:stop(P)
     end || P <- Acceptors],
    gen_paxos_proposer:stop(Pid),
    gen_paxos_learner:stop(L).
    
    
