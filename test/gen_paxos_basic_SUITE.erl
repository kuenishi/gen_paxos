-module(gen_paxos_basic_SUITE).

-compile(export_all).

all() ->
    [start_stop,
    %%  slaves
     noop
    ].


start_stop(_) ->
    application:set_env(gen_paxos, nodes, [node()]),
    ok = application:start(gen_paxos),
    %% ct:log(boom, "boom~n"),
    %% Testing n_val = 1 case
    Res = gen_paxos_acceptor:status(),
    ct:log(me, "status: ~p", [Res]),
    %% ct:fail(me),
    ok = application:stop(gen_paxos).

slaves(_) ->
    {ok, CWD} = file:get_cwd(),
    Path = filename:join([CWD, "..", "..", ".."]),
    {ok, N1} = ct_slave:start(n1, [{env, [{"ERL_LIBS", Path}]}]),
    %% Res0 = rpc:call(N1, code, add_path, []),
    Res0 = ok = rpc:call(N1, application, start, [gen_paxos]),
    ct:log(master, "N1 started: ~p, ~p", [N1, {Path, Res0}]),
    ok = rpc:call(N1, gen_server, call, [gen_paxos_acceptor, status]),
    Res = ok = gen_server:call({gen_paxos_acceptor, N1}, hello),
    ct:log(master, "Remote call: ~p", [Res]),
    {ok, _} = ct_slave:stop(n1),
    ok.

noop(_) ->
    ok.
