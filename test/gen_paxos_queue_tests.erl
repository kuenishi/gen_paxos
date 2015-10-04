-module(gen_paxos_queue_tests).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

queue_test() ->
    Name = erlang:make_ref(),
    {ok, P1} = gen_paxos_queue:start_link(Name, 1),
    {ok, P2} = gen_paxos_queue:start_link(Name, 2),
    {ok, M1} = gen_paxos_queue:get_ballot_manager(P1),
    {ok, M2} = gen_paxos_queue:get_ballot_manager(P2),
    CSet = [M1, M2],
    ok = gen_paxos_queue:boot(P1, CSet),
    ok = gen_paxos_queue:boot(P2, CSet),

    [begin
         Item = {N, span},
         push_until_ok(P1, Item, 0),
         timer:sleep(1),
         ?assertEqual({ok, Item}, gen_paxos_queue:top(P1)),
         ?assertEqual({ok, Item}, gen_paxos_queue:top(P2)),
         ?assertEqual({ok, Item}, gen_paxos_queue:pop(P1)),
         timer:sleep(1),
         ?assertEqual({error, empty}, gen_paxos_queue:top(P2))
     end || N <- lists:seq(1, 200)],

    ok = gen_paxos_queue:stop(P1),
    ok = gen_paxos_queue:stop(P2).

push_until_ok(P, Term, Count) ->
    case gen_paxos_queue:push(P, Term) of
        ok -> ok;
        {error, retry} ->
            push_until_ok(P, Term, Count+1);
        {error, conflict} ->
            ?debugVal({conflict, Term}),
            push_until_ok(P, Term, Count+1)
    end.

pop_until_ok(P, Count) ->
    case gen_paxos_queue:pop(P) of
        {ok, _} = R -> R;
        {error, retry} ->
            pop_until_ok(P, Count+1);
        {error, conflict} ->
            ?debugVal(conflict),
            pop_until_ok(P, Count+1)
    end.


microbench_queue_test() ->
    Name = erlang:make_ref(),
    {ok, P1} = gen_paxos_queue:start_link(Name, 1),
    {ok, P2} = gen_paxos_queue:start_link(Name, 2),
    {ok, P3} = gen_paxos_queue:start_link(Name, 3),
    {ok, M1} = gen_paxos_queue:get_ballot_manager(P1),
    {ok, M2} = gen_paxos_queue:get_ballot_manager(P2),
    {ok, M3} = gen_paxos_queue:get_ballot_manager(P3),
    CSet = [M1, M2, M3],
    ok = gen_paxos_queue:boot(P1, CSet),
    ok = gen_paxos_queue:boot(P2, CSet),
    ok = gen_paxos_queue:boot(P3, CSet),
    PSet = [P1, P2, P3],

    N = 512,
    {T0, Answer}
        = timer:tc(fun() ->
                           [begin
                                Item = {I, span},
                                P = lists:nth((-I) rem 3 + 3, PSet),
                                push_until_ok(P, Item, 0),
                                %% ?debugVal({I, Item}),
                                Item
                            end || I <- lists:seq(1, N)]
                   end),
    ?debugFmt("Microbench: ~p push/sec",
              [N * 1000000.0 / T0]),
    {T1, Result}
        = timer:tc(fun() ->
                           [begin
                                %% Item = {N, span},
                                P = lists:nth(I rem 3 + 1, PSet),
                                {ok, {_, span} = Item} = pop_until_ok(P, 0),
                                %% ?debugVal({I, Item}),
                                Item
                            end || I <- lists:seq(1, N)]
                   end),
    ?debugFmt("Microbench: ~p pop/sec",
              [N * 1000000.0 / T1]),

    ?assertEqual(N, length(Result)),
    ?assertEqual(length(Answer), length(Result)),

    %% Check the popped value are always sequencial; TODO: BUG: XXX:
    %% FIXME: This shows a race condition where a popped value is not
    %% sequencial :'-S
    case Result of
        Answer -> ?debugVal("Answer==Result");
        _ ->
            [case R of
                 A -> noop;
                 _ -> ?debugVal({A, R})
             end || {A, R} <- lists:zip(Answer, Result)],
            ?assertEqual(Answer, Result)
    end,
                         
    ok = gen_paxos_queue:stop(P1),
    ok = gen_paxos_queue:stop(P2).



