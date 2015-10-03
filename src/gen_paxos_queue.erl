%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2015, UENISHI Kota
%%% @doc

%% Perfectly serialized queue. Still unknown under network partition;
%% TODO: replace/3 is not yet implemented.

%%% @end
%%% Created :  3 Jul 2015 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(gen_paxos_queue).

-behaviour(gen_paxos_serialized_data).

%% API: for now, all API are non-blocking
-export([start_link/2,
         get_ballot_manager/1,
         boot/2,
         push/2,
         top/1,
         pop/1,
         stop/1]).

-export([handle_op/3, handle_tap/3]).

-include_lib("eunit/include/eunit.hrl").

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Signature, Base) ->
    gen_paxos_serialized_data:start_link(Signature, Base,
                                         ?MODULE, queue:new()).

boot(Pid, ConsensuSet) ->
    gen_paxos_serialized_data:boot(Pid, ConsensuSet).

get_ballot_manager(Pid) ->
    gen_paxos_serialized_data:get_ballot_manager(Pid).

push(Pid, Term) ->
    gen_paxos_serialized_data:call(Pid, {push, Term}).

top(Pid) ->
    gen_paxos_serialized_data:tap(Pid, top).

pop(Pid) ->
    gen_paxos_serialized_data:call(Pid, pop).

stop(Pid) ->
    gen_server:call(Pid, stop).
    
%% replace(_, _) ->
%%     ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

handle_op(pop, _, Q0) ->
    case queue:out(Q0) of
        {{value, Item}, Qx} ->
            %% ?debugVal({pop, Epoch, Item}),
            {{ok, Item}, Qx};
        {empty, Q0} ->
            {{error, empty}, Q0}
    end;
handle_op({push, Item}, _, Q0) ->
    %% ?debugVal({Epoch, Item}),
    {ok, queue:in(Item, Q0)}.
    
handle_tap(top, _, Q0) ->    
    case queue:out(Q0) of
        {{value, Item}, _} ->
            {ok, Item};
        {empty, Q0} ->
            {error, empty}
    end.
