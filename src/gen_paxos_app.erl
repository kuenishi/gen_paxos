-module(gen_paxos_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    gen_paxos_sup:start_link().

stop(_State) ->
    ok.

-ifdef(TEST).
some_test() ->
    ok.
-endif.
