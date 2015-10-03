%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2015, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created :  3 Jul 2015 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(gen_paxos_map).

-behaviour(gen_paxos_serialized_data).

%% API
-export([start_link/2, get_ballot_manager/1,
         boot/2,
         put/3, get/2, delete/3, update/4,
         replace/2]).

%% gen_server callbacks
-export([handle_op/3, handle_tap/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name, Base) ->
    gen_paxos_serialized_data:start_link(Name, Base,
                                         ?MODULE,
                                         orddict:new()).

get_ballot_manager(Pid) ->
    gen_paxos_serialized_data:get_ballot_manager(Pid).

boot(Pid, CSet) ->
    gen_paxos_serialized_data:boot(Pid, CSet).

put(Pid, Key, Value) ->
    gen_paxos_serialized_data:call(Pid, {put, Key, Value}).

get(Pid, Key) ->
    gen_paxos_serialized_data:tap(Pid, {get, Key}).

delete(Pid, Key, Vsn) ->
    gen_paxos_serialized_data:call(Pid, {delete, Key, Vsn}).

update(Pid, Key, Vsn, Value) ->
    gen_paxos_serialized_data:call(Pid, {udpate, Key, Vsn, Value}).

replace(_, _) ->
    not_yet.

%%%===================================================================
%%% gen_paxos_serialized_data callbacks
%%%===================================================================

handle_op({put, Key, Value}, _, D0) ->
    D1 = orddict:udpate(Key,
                        fun({_, Vsn}) -> {Value, Vsn+1} end,
                        {nope, 0},
                        D0),
    {ok, D1};

handle_op({delete, Key, Vsn}, _, D0) ->
    case orddict:find(Key, D0) of
        error -> {error, notfound};
        {ok, {_Value, Vsn}} ->
            {ok, orddict:erase(Key, D0)};
        {ok, {_Value, RealVsn}} ->
            {{error, RealVsn}, D0}
    end;

handle_op({update, Key, Vsn, Value}, _, D0) ->
    case orddict:find(Key, D0) of
        error when Vsn =:= 0 ->
            {{ok, 1},
             orddict:store(Key, {Value, 1}, D0)};
        error ->
            {{error, notfound},
             D0};
        {ok, {_, Vsn}} ->
            {{ok, Vsn+1},
             orddict:store(Key, {Value, Vsn+1}, D0)};
        {ok, {_, RealVsn}} ->
            {{error, RealVsn}, D0}
    end.

handle_tap({get, Key}, _, D0) ->
    orddict:find(Key, D0).
