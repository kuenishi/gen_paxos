-module(leader_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

leader_test() ->
    Signature = make_ref(),
    {ok, P1} = gen_paxos_leader:start_link(Signature, 1,
                                           [{lease_time_sec, 1}]),
    {ok, M1} = gen_paxos_leader:get_ballot_manager(P1),
    {ok, P2} = gen_paxos_leader:start_link(Signature, 2,
                                           [{lease_time_sec, 1}]),
    {ok, M2} = gen_paxos_leader:get_ballot_manager(P2),

    CSet = [M2, M1],
    ok = gen_paxos_leader:boot(P1, CSet),
    ok = gen_paxos_leader:boot(P2, CSet),
    ?debugVal(gen_paxos_leader:status(P1)),
    ?debugVal(gen_paxos_leader:status(P2)),
    [begin
         timer:sleep(5),
         {ok, L1} = gen_paxos_leader:leader(P1),
         ?assertEqual({ok, L1}, gen_paxos_leader:leader(P2))
     end || _ <- lists:seq(1, 100)],
    ok = gen_paxos_leader:stop(P2),
    ok = gen_paxos_leader:stop(P1),
    ?debugHere,
    ok.
    
many_candidates_test() ->
    Signature = make_ref(),
    NumNodes = 37,
    Ps = [begin
              {ok, P} = gen_paxos_leader:start_link(Signature, N,
                                                    [{lease_time_sec, 1}]),
              {ok, M} = gen_paxos_leader:get_ballot_manager(P),
              {P, M}
          end || N <- lists:seq(1, NumNodes)],
    CSet = [M || {_, M} <- Ps],
    [ok = gen_paxos_leader:boot(Pn, CSet) || {Pn, _} <- Ps],
    %% ?debugVal(gen_paxos_leader:status(P1)),
    %% ?debugVal(gen_paxos_leader:status(P2)),
    timer:sleep(5),
    [begin
         Ls = [begin
                   {ok, L} = gen_paxos_leader:leader(Pn),
                   L
               end || {Pn, _} <- Ps],
         %%?debugFmt("~w", [Ls]),
         [_] = Sorted = lists:usort(Ls),
         ?assertEqual(1, length(Sorted))
     end || _N <- lists:seq(1, 10)],
    [ok = gen_paxos_leader:stop(Pn) || {Pn, _} <- Ps],
    ?debugHere,
    ok.
