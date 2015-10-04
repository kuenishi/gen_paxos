-module(gen_paxos_ballot_manager_tests).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

leader_test() ->
    Signature = make_ref(),
    {ok, P1} = gen_paxos_ballot_manager:start_link(Signature, 1),
    {ok, P2} = gen_paxos_ballot_manager:start_link(Signature, 2),
    CSet = [P2, P1],
    ok = gen_paxos_ballot_manager:boot(P1, CSet),
    ok = gen_paxos_ballot_manager:boot(P2, CSet),
    ?debugVal(gen_paxos_ballot_manager:status(P1)),
    ?debugVal(gen_paxos_ballot_manager:status(P2)),

    timer:sleep(110),
    [begin
         PropValue = {pocketburger, Epoch * 23},
         P = lists:nth(Epoch rem 2 + 1, CSet),
         {ok, started} = gen_paxos_ballot_manager:ballot(P, Epoch,
                                                         PropValue),
         timer:sleep(7),
         {ok, PeekValue} = gen_paxos_ballot_manager:peek(P2, Epoch),
         {ok, PeekValue} = gen_paxos_ballot_manager:peek(P1, Epoch),
         %% ?debugVal(PeekValue),
         ?assertEqual({pocketburger, Epoch * 23}, PeekValue),
         case Epoch - 100 of
             E when E > 0 ->
                 {ok, D0} = gen_paxos_ballot_manager:trim(P1, E),
                 {ok, D1} = gen_paxos_ballot_manager:trim(P2, E),
                 ?assertEqual(D0, D1);
             _ ->
                 noop
         end

     end || Epoch <- lists:seq(1, 328)],

    ok = gen_paxos_ballot_manager:stop(P2),
    ok = gen_paxos_ballot_manager:stop(P1),
    ok.

replace_test() ->
    ?debugHere,
    Signature = make_ref(),
    {ok, P1} = gen_paxos_ballot_manager:start_link(Signature, 1),
    {ok, P2} = gen_paxos_ballot_manager:start_link(Signature, 2),
    CSet = [P2, P1],
    ok = gen_paxos_ballot_manager:boot(P1, CSet),
    ok = gen_paxos_ballot_manager:boot(P2, CSet),
    ?debugVal(gen_paxos_ballot_manager:status(P1)),
    ?debugVal(gen_paxos_ballot_manager:status(P2)),

    {ok, P3} = gen_paxos_ballot_manager:start_link(Signature, 1, 20),

    %% Tell P2 to replace P1 with P3
    %% TODO: current replace design is broken; like it simulates
    %% omission failure or netsplit!!!
    ok = gen_paxos_ballot_manager:replace(P2, P1, {P3, 20}),
    
    timer:sleep(110),

    %% Make sure P1 got dead
    ok = gen_paxos_ballot_manager:stop(P1),

    CSet2 = [P3, P2],

    [begin
         PropValue = {pocketburger, Epoch * 23},
         P = lists:nth(Epoch rem 2 + 1, CSet2),
         {ok, started} = gen_paxos_ballot_manager:ballot(P, Epoch,
                                                         PropValue),
         timer:sleep(7),
         {ok, PeekValue} = gen_paxos_ballot_manager:peek(P3, Epoch),
         {ok, PeekValue} = gen_paxos_ballot_manager:peek(P2, Epoch),
         ?debugVal(PeekValue),
         ?assertEqual({pocketburger, Epoch * 23}, PeekValue),
         case Epoch - 100 of
             E when E > 0 ->
                 {ok, _D0} = gen_paxos_ballot_manager:trim(P3, E),
                 {ok, _D1} = gen_paxos_ballot_manager:trim(P2, E);
             
             _ ->
                 noop
      end

end || Epoch <- lists:seq(20, 40)],

    ok = gen_paxos_ballot_manager:stop(P3),
    ok = gen_paxos_ballot_manager:stop(P2),
    ok.
