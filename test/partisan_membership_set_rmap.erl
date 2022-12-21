%% -------------------------------------------------------------------
%% Bases on riak_dt_map: OR-Set schema based multi CRDT container
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc
%% @end

-module(partisan_membership_set_rmap).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-record(partisan_membership_set, {
    version = 1         ::  integer(),
    entries = #{}       ::  entries(),
    deferred = #{}      ::  deferred(),
    context             ::  context()
}).

-opaque t()             :: #partisan_membership_set{}.
-type vclock()          :: partisan_vclock:vclock().
-type context()         :: vclock() | undefined.
-type entries()         :: #{nodename() => entry_value()}.
%% Only field removals can be deferred.
-type deferred()        :: #{context() => [nodename()]}.
-type nodename()        :: atom().
%% Only for present fields, ensures removes propagate
-type entry_value()     :: {dotmap(), tombstone()}.
-type dotmap()          :: #{dot() => value()}.
-type value()           :: node_value() | tombstone().
-type node_value()      :: {node_spec(), Ts :: non_neg_integer()}.
-type tombstone()       :: {undefined, 0}.
-type dot()             :: {Actor :: nodename(), pos_integer()}.
-type op()              :: {add, node_spec()} | {remove, node_spec()}.
-type not_member()      :: {not_member, nodename()}.

-export_type([t/0]).
-export_type([actor/0]).
-export_type([context/0]).
-export_type([value/0]).
-export_type([dot/0]).
-export_type([op/0]).


%% API
-export([add/3]).
-export([decode/1]).
-export([encode/1]).
-export([equal/2]).
-export([merge/2]).
-export([new/0]).
-export([remove/3]).
-export([to_list/1]).


-export([precondition_context/1]).
-export([stat/2]).
-export([stats/1]).



%% =============================================================================
%% API
%% =============================================================================




%% -----------------------------------------------------------------------------
%% @doc Create a new, empty Map.
%% @end
%% -----------------------------------------------------------------------------
-spec new() -> t().

new() ->
    #partisan_membership_set{context = partisan_vclock:fresh()}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode(t()) -> binary().

encode(#partisan_membership_set{} = T) ->
    Opts = case partisan_config:get(membership_binary_compression, 1) of
        true ->
            [compressed];
        N when N >= 0, N =< 9 ->
            [{compressed, N}];
        _ ->
            []
    end,

    erlang:term_to_binary(T, Opts).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec decode(binary()) -> t().

decode(Binary) ->
    erlang:binary_to_term(Binary).


%% -----------------------------------------------------------------------------
%% @doc get the current set of values for this Map
%% @end
%% -----------------------------------------------------------------------------
-spec to_list(t()) -> [node_spec()].

to_list(#partisan_membership_set{entries = Entries}) ->
   lists:reverse(
       maps:fold(
            fun(_, {_DotMap, _Tombstone} = EntryValue, Acc) ->
                {Value, _} = merge_entry_values(EntryValue),
                [Value|Acc]
            end,
            [],
            Entries
        )
    ).


%% -----------------------------------------------------------------------------
%% @doc update the `t()' or a field in the `t()' by
%% executing the `map_op()'. `Ops' is a list of one or more of the
%% following ops:
%%
%% If there is no local value for `Key' a new CRDT is created, the operation applied
%% and the result inserted otherwise, the operation is applied to the
%% local value.
%%
%% Atomic, all of `Ops' are performed successfully, or none are.
%% @end
%% -----------------------------------------------------------------------------
-spec add(node_spec(), nodename() | dot(), t()) ->
    {ok, t()} | {error, not_member()}.

add(#{name := _} = NodeSpec, ActorOrDot, T) ->
    update([{add, NodeSpec}], ActorOrDot, T, undefined).


%% -----------------------------------------------------------------------------
%% @doc
%% `{remove, `nodename()'}' where field is `{name, type}', results in
%%  the crdt at `field' and the key and value being removed. A
%%  concurrent `update' will "win" over a remove so that the field is
%%  still present, and it's value will contain the concurrent update.
%% @end
%% -----------------------------------------------------------------------------
-spec remove(node_spec() | nodename(), nodename() | dot(), t()) ->
    {ok, t()} | {error, not_member()}.

remove(#{name := Nodename}, ActorOrDot, T) ->
    remove(Nodename, ActorOrDot, T);

remove(Nodename, ActorOrDot, T) ->
    update([{remove, Nodename}], ActorOrDot, T, undefined).



%% -----------------------------------------------------------------------------
%% @doc merge two `t()'s.
%% @end
%% -----------------------------------------------------------------------------
-spec merge(t(), t()) -> t().

merge(T, T) ->
    T;

merge(
    #partisan_membership_set{context = C1, entries = E1, deferred = D1},
    #partisan_membership_set{context = C2, entries = E2, deferred = D2}) ->
    %% @TODO is there a way to optimise this, based on clocks maybe?
    Clock = partisan_vclock:merge([C1, C2]),
    {Common, Unique1, Unique2} = nodename_sets(E1, E2),
    Acc0 = filter_unique(Unique1, E1, C2, maps:new()),
    Acc1 = filter_unique(Unique2, E2, C1, Acc0),
    Entries = merge_common(Common, E1, E2, C1, C2, Acc1),
    Deferred = merge_deferred(D2, D1),

    T = #partisan_membership_set{
        context = Clock,
        entries = Entries,
        deferred = maps:new()
    },

    %% apply those deferred field removals, if they're
    %% preconditions have been met, that is.
    maps:fold(
        fun(Ctx, Nodenames, Acc) -> remove_all(Nodenames, Acc, Ctx) end,
        T,
        Deferred
    ).


%% -----------------------------------------------------------------------------
%% @doc compare two `t()'s for equality of structure Both
%% schemas and value list must be equal. Performs a pariwise equals for
%% all values in the value lists
%% @end
%% -----------------------------------------------------------------------------
-spec equal(t(), t()) -> boolean().

equal(
    #partisan_membership_set{context = C1, entries = E1, deferred = D1},
    #partisan_membership_set{context = C2, entries = E2, deferred = D2}) ->

    partisan_vclock:equal(C1, C2)
        andalso D1 == D2
        andalso pairwise_equals(maps:to_list(E1), maps:to_list(E2)).


%% -----------------------------------------------------------------------------
%% @doc an opaque context that can be passed to `update/4' to ensure
%% that only seen fields are removed. If a field removal operation has
%% a context that the Map has not seen, it will be deferred until
%% causally relevant.
%% @end
%% -----------------------------------------------------------------------------
-spec precondition_context(t()) -> context().

precondition_context(#partisan_membership_set{context = Clock}) ->
    Clock.


%% -----------------------------------------------------------------------------
%% @doc stats on internal state of Map.
%% A proplist of `{StatName :: atom(), Value :: integer()}'. Stats exposed are:
%% `actor_count': The number of actors in the clock for the Map.
%% `member_count': The total number of fields in the Map (including divergent field entries).
%% `duplication': The number of duplicate entries in the Map across all fields.
%%                basically `member_count' - ( unique fields)
%% `deferred_length': How many operations on the deferred list, a reasonable expression
%%                   of lag/staleness.
%% @end
%% -----------------------------------------------------------------------------
-spec stats(t()) -> [{atom(), integer()}].

stats(#partisan_membership_set{} = T) ->
    [
        {S, stat(S, T)}
        || S <- [actor_count, member_count, duplication, deferred_length]
    ].


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec stat(atom(), t()) -> number() | undefined.

stat(actor_count, #partisan_membership_set{context = Clock}) ->
    length(Clock);

stat(deferred_length, #partisan_membership_set{deferred = Map}) ->
    maps:size(Map);

stat(duplication, #partisan_membership_set{entries = Map}) ->
    %% Number of duplicated fields
    {NodeCnt, Duplicates} = maps:fold(
        fun(_Nodename, {DotMap ,_}, {FCnt, DCnt}) ->
            {FCnt + 1, DCnt + maps:size(DotMap)}
        end,
        {0, 0},
        Map
    ),
    Duplicates - NodeCnt;

stat(member_count, #partisan_membership_set{entries = Map}) ->
    maps:size(Map);

stat(_,_) ->
    undefined.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @private
%% @doc update the `t()' or a field in the `t()' by
%% executing the `map_op()'. `Ops' is a list of one or more of the
%% following ops:
%%
%% `{update, nodename(), Op} where `Op' is a valid update operation for a
%% CRDT of type `Mod' from the `Key' pair `{Name, Mod}' If there is no
%% local value for `Key' a new CRDT is created, the operation applied
%% and the result inserted otherwise, the operation is applied to the
%% local value.
%%
%%  `{remove, `nodename()'}' where field is `{name, type}', results in
%%  the crdt at `field' and the key and value being removed. A
%%  concurrent `update' will "win" over a remove so that the field is
%%  still present, and it's value will contain the concurrent update.
%%
%% Atomic, all of `Ops' are performed successfully, or none are.
%%
%% If context is =/= undefined works the context ensures no
%% unseen field updates are removed, and removal of unseen updates is
%% deferred. The Context is passed down as the context for any nested
%% types. hence the common clock.
%%
%% @end
%% -----------------------------------------------------------------------------
-spec update(Ops :: [op()], nodename() | dot(), t(), context()) ->
    {ok, t()} | {error, not_member()}.

update(Ops, ActorOrDot, #partisan_membership_set{} = T0, Ctx) ->
    Clock0 = T0#partisan_membership_set.context,
    {Dot, Clock} = update_clock(ActorOrDot, Clock0),
    T1 = T0#partisan_membership_set{context = Clock},

    try
        T = apply_ops(Ops, Dot, T1, Ctx),
        {ok, T}
    catch
        throw:Reason ->
            {error, Reason}
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc break the keys from an two maps out into three sets, the
%% common keys, those unique to one, and those unique to the other.
%% @end
%% -----------------------------------------------------------------------------
-spec nodename_sets(entries(), entries()) ->
    {ordsets:ordset(), ordsets:ordset(), ordsets:ordset()}.

nodename_sets(A, B) ->
    ASet = ordsets:from_list(maps:keys(A)),
    BSet = ordsets:from_list(maps:keys(B)),
    {
        ordsets:intersection(ASet, BSet),
        ordsets:subtract(ASet, BSet),
        ordsets:subtract(BSet, ASet)
    }.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec pairwise_equals(
    [{nodename(), entry_value()}], [{nodename(), entry_value()}]) ->
    boolean().

pairwise_equals([], []) ->
    true;

pairwise_equals(
    [{Nodename, {Dotmap1, Tomb1}}| Rest1],
    [{Nodename, {Dotmap2, Tomb2}}|Rest2]) ->
    %% Tombstones don't need to be equal. When we merge with a map
    %% where one side is absent, we take the absent sides clock, when
    %% we merge where both sides have a field, we merge the
    %% tombstones, and apply deferred. The deferred remove uses a glb
    %% of the context and the clock, meaning we get a smaller
    %% tombstone. Both are correct when it comes to determining the
    %% final value. As long as tombstones are not conflicting (that is
    %% A == B | A > B | B > A)
    case {maps:keys(Dotmap1) == maps:keys(Dotmap2), Tomb1 == Tomb2} of
        {true, true} ->
            pairwise_equals(Rest1, Rest2);
        _ ->
            false
    end;

pairwise_equals(_, _) ->
    false.


%% -----------------------------------------------------------------------------
%% @private
%% @doc for a set of dots (that are unique to one side) decide
%% whether to keep, or drop each.
%% @end
%% -----------------------------------------------------------------------------
-spec filter_dots(ordesets:ordset(), dotmap(), vclock()) ->
    dotmap().

filter_dots(Dots, DotMap, Clock) ->
    DotsToKeep = ordsets:filter(
        fun(Dot) ->
            is_dot_unseen(Dot, Clock)
        end,
        Dots
    ),

    maps:filter(
        fun(Dot, _CRDT) ->
            ordsets:is_element(Dot, DotsToKeep)
        end,
        DotMap
    ).



%% -----------------------------------------------------------------------------
%% @private
%% @doc merge the common fields into a set of surviving dots and a
%% tombstone per field.  If a dot is on both sides, keep it. If it is
%% only on one side, drop it if dominated by the otherside's clock.
%% @end
%% -----------------------------------------------------------------------------
merge_common(Nodenames, AEntries, BEntries, AClock, BClock, Acc) ->
    ordsets:fold(fun(Nodename, Keep) ->
        {ADotMap, ATomb} = maps:get(Nodename, AEntries),
        {BDotMap, BTomb} = maps:get(Nodename, BEntries),

        {CommonDots, AUnique, BUnique} = nodename_sets(ADotMap, BDotMap),

        Tomb = merge_values(BTomb, ATomb),

        CommonSurviving = ordsets:fold(
            fun(Dot, Common) ->
                EntryValue = maps:get(Dot, ADotMap),
                maps:put(Dot, EntryValue, Common)
            end,
            maps:new(),
            CommonDots
        ),

        ASurviving = filter_dots(AUnique, ADotMap, BClock),
        BSurviving = filter_dots(BUnique, BDotMap, AClock),

        DotMap = maps:merge(
            maps:merge(BSurviving, ASurviving), CommonSurviving
        ),

        case maps:size(DotMap) of
            0 ->
                Keep;
            _ ->
                maps:put(Nodename, {DotMap, Tomb}, Keep)
        end

    end,
    Acc,
    Nodenames
).


%% -----------------------------------------------------------------------------
%% @private
%% %% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec merge_deferred(deferred(), deferred()) -> deferred().

merge_deferred(A, B) ->
    maps:merge_with(
        fun(_, AValue, BValue) -> ordsets:union(AValue, BValue) end,
        A,
        B
    ).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec remove_all([nodename()], t(), context()) -> t().

remove_all(Nodenames, T, Ctx) ->
    lists:foldl(
        fun(Nodename, Acc) ->
            remove_node(Nodename, Acc, Ctx)
        end,
        T,
        Nodenames
    ).


%% -----------------------------------------------------------------------------
%% @private
%% @doc update the clock, and get a dot for the operations. This
%% means that field removals increment the clock too.
%% @end
%% -----------------------------------------------------------------------------
-spec update_clock(nodename() | dot(), vclock()) -> {dot(), vclock()}.

update_clock(Dot, Clock) when is_tuple(Dot) ->
    NewClock = partisan_vclock:merge([[Dot], Clock]),
    {Dot, NewClock};

update_clock(Actor, Clock) ->
    NewClock = partisan_vclock:increment(Actor, Clock),
    Dot = {Actor, partisan_vclock:get_counter(Actor, NewClock)},
    {Dot, NewClock}.



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec apply_ops([op()], dot(), t(), context()) ->
    t() | no_return().

apply_ops([], _, T, _) ->
    T;

apply_ops([{add, #{name := Nodename} = NodeSpec} | Rest], Dot, T0, Ctx)
when is_tuple(Dot) ->
    Entries0 = T0#partisan_membership_set.entries,

    %% Merge entry for nodename is present or return new if not
    EntryValue = case maps:find(Nodename, Entries0) of
        {ok, {DotMap0, _Tomb0} = EntryValue0} ->
            %% We have existing versions
            Merged = merge_entry_values(EntryValue0),
            Updated = update_value(NodeSpec, Merged),
            DotMap = maps:put(Dot, Updated, DotMap0),
            {DotMap, tombstone()};

        error ->
            {#{Dot => new_value(NodeSpec)}, tombstone()}
    end,

    Entries = maps:put(Nodename, EntryValue, Entries0),
    T = T0#partisan_membership_set{entries = Entries},
    apply_ops(Rest, Dot, T, Ctx);

apply_ops([{remove, Node} | Rest], Dot, T0, Ctx) ->
    T1 = remove_node(Node, T0, Ctx),
    apply_ops(Rest, Dot, T1, Ctx).



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec merge_entry_values(entry_value()) -> value().

merge_entry_values({DotMap0, Tomb0}) ->
    Merged0 = maps:fold(
        fun(_Dot, Value, Acc) -> merge_values(Value, Acc) end,
        tombstone(),
        DotMap0
    ),
    %% Merge with the tombstone to drop any removed dots
    merge_values(Tomb0, Merged0).



%% -----------------------------------------------------------------------------
%% @private
%% @doc This is the Least Upper Bound function described in the literature.
%% @end
%% -----------------------------------------------------------------------------
-spec merge_values(value(), value()) -> value().

merge_values({Val1, TS1}, {_Val2, TS2}) when TS1 > TS2 ->
    {Val1, TS1};

merge_values({_Val1, TS1}, {Val2, TS2}) when TS2 > TS1 ->
    {Val2, TS2};

merge_values({_, _} = Val1, {_, _} = Val2) when Val1 >= Val2 ->
    Val1;

merge_values({_, _}, {_, _} = Val2) ->
    Val2.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec tombstone() -> tombstone().

tombstone() ->
    {undefined, 0}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new_value(node_spec()) -> node_value().

new_value(Nodespec) ->
    {Nodespec, erlang:system_time(microsecond)}.



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new_value(node_spec(), non_neg_integer()) -> node_value().

new_value(Nodespec, Timestamp) ->
    {Nodespec, Timestamp}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update_value(value(), value()) -> value().

update_value(NodeSpec, {_, Ts} = OldValue) ->
    Now = erlang:system_time(microsecond),

    case Now > Ts of
        true ->
            new_value(NodeSpec, Now);
        false ->
            OldValue
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc when context is undefined, we simply remove all instances
%% of nodename, regardless of their dot. If the nodename is not present then
%% we warn the user with a precondition error. However, in the case
%% that a context is provided we can be more fine grained, and only
%% remove those nodename entries whose dots are seen by the context. This
%% preserves the "observed" part of "observed-remove". There is no
%% precondition error if we're asked to remove smoething that isn't
%% present, either we defer it, or it has been done already, depending
%% on if the Map clock descends the context clock or not.
%%
%% {@link defer_remove_node/4} for handling of removes of fields that are
%% _not_ present
%% @end
%% -----------------------------------------------------------------------------
-spec remove_node(nodename() | node_spec(), t(), context()) ->
    t() | no_return().

remove_node(#{name := Nodename}, T, Ctx) ->
    remove_node(Nodename, T, Ctx);

remove_node(Nodename, #partisan_membership_set{} = T0, undefined) ->
    Entries0 = T0#partisan_membership_set.entries,

    case maps:take(Nodename, Entries0) of
        {_Removed, Entries} ->
            T0#partisan_membership_set{entries = Entries};

        error ->
            throw({not_member, Nodename})
    end;

remove_node(Nodename, #partisan_membership_set{} = T0, Ctx) ->
    Entries0 = T0#partisan_membership_set.entries,
    Clock = T0#partisan_membership_set.context,

    T1 = defer_remove_node(Nodename, T0, Ctx),

    Entries = case ctx_rem_node(Nodename, Entries0, Ctx, Clock) of
        empty ->
            maps:remove(Nodename, Entries0);
        CRDTs ->
            maps:put(Nodename, CRDTs, Entries0)
    end,

    T1#partisan_membership_set{entries = Entries}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc drop dominated fields
%% @end
%% -----------------------------------------------------------------------------
ctx_rem_node(Nodename, Entries, Ctx, _Clock) ->
    case maps:find(Nodename, Entries) of
        {ok, {DotMap0, Tomb0}} ->
            %% Drop dominated fields, and update the tombstone.
            %%
            %% If the context is removing a field at dot {a, 1} and the
            %% current field is {a, 2}, the tombstone ensures that all events
            %% from {a, 1} are removed from the crdt value. If the ctx remove
            %% is at {a, 3} and the current field is at {a, 2} then we need to
            %% remove only events up to {a, 2}. The glb clock enables that.
            %%
            %% GLB is events seen by both clocks only
            % TombClock = partisan_vclock:glb(Ctx, Clock),

            Tomb = tombstone(),

            DotMap = maps:filter(
                fun(Dot, _Value) -> is_dot_unseen(Dot, Ctx) end,
                DotMap0
            ),
            case maps:size(DotMap) of
                0 ->
                    %% Ctx remove removed all dots for field
                    empty;
                _ ->
                    %% Update the tombstone with the GLB clock
                    {DotMap, merge_values(Tomb, Tomb0)}
            end;

        error ->
            empty
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc If we're asked to remove something we don't have (or have,
%% but maybe not all 'updates' for it), is it because we've not seen
%% the some update that we've been asked to remove, or is it because
%% we already removed it? In the former case, we can "defer" this
%% operation by storing it, with its context, for later execution. If
%% the clock for the Map descends the operation clock, then we don't
%% need to defer the op, its already been done. It is _very_ important
%% to note, that only _actorless_ operations can be saved. That is
%% operations that DO NOT need to increment the clock. In a Map this
%% means field removals only. Contexts for update operations do not
%% result in deferred operations on the parent Map. This simulates
%% causal delivery, in that an `update' must be seen before it can be
%% `removed'.
%% @end
%% -----------------------------------------------------------------------------
-spec defer_remove_node(nodename(), t(), vclock()) -> t().

defer_remove_node(Nodename, T0, Ctx) ->
    Deferred0 = T0#partisan_membership_set.deferred,
    Clock = T0#partisan_membership_set.context,

    case partisan_vclock:descends(Clock, Ctx) of
        true ->
            %% no need to save this remove, we're done
            T0;
        false ->
            Deferred = maps:update_with(
                Ctx,
                fun(Nodenames) ->
                    ordsets:add_element(Nodename, Nodenames)
                end,
                ordsets:add_element(Nodename, ordsets:new()),
                Deferred0
            ),

            T0#partisan_membership_set{deferred = Deferred}
    end.



%% -----------------------------------------------------------------------------
%% @private
%% @doc filter the set of fields that are on one side of a merge only.
%% @end
%% -----------------------------------------------------------------------------
-spec filter_unique(
    ordsets:ordset(), entries(), vclock(), entries()) -> entries().

filter_unique(Nodenames, Entries, Clock, Acc) ->
    ordsets:fold(
        fun(Nodename, IAcc) ->
            {DotMap0, Tombstone0} = maps:get(Nodename, Entries),

            DotMap = maps:filter(
                fun(Dot, _CRDT) ->
                    is_dot_unseen(Dot, Clock)
                end,
                DotMap0
            ),

            case maps:size(DotMap) of
                0 ->
                    IAcc;
                _ ->
                    %% create a tombstone since the
                    %% otherside does not have this nodename,
                    %% it either removed it, or never had
                    %% it. If it never had it, the removing
                    %% dots in the tombstone will have no
                    %% impact on the value, if the otherside
                    %% removed it, then the removed dots
                    %% will be propagated by the tombstone.
                    Tombstone = merge_values(Tombstone0, tombstone()),
                    maps:put(Nodename, {DotMap, Tombstone}, IAcc)
            end
    end,
    Acc,
    Nodenames
).


%% -----------------------------------------------------------------------------
%% @private
%% @doc predicate function, `true' if the provided `dot()' is
%% concurrent with the clock, `false' if the clock has seen the dot.
%% @end
%% -----------------------------------------------------------------------------
-spec is_dot_unseen(dot(), vclock()) -> boolean().

is_dot_unseen(Dot, Clock) ->
    not partisan_vclock:descends(Clock, [Dot]).



%% =============================================================================
%% EUNIT TESTS
%% =============================================================================



-ifdef(TEST).

update(Actions, ActorOrDot, T) ->
    update(Actions, ActorOrDot, T, undefined).


node_spec(Nodename) ->
    node_spec(Nodename, {127,0,0,1}).


node_spec(Nodename, IP) ->
    #{
        channels => [undefined],
        listen_addrs => [#{ip => IP, port => 62823}],
        name => Nodename,
        parallelism => 1
    }.

update_test() ->
    Nodename = 'node1@127.0.0.1',
    Node1 = node_spec(Nodename),
    Node2 = node_spec(Nodename, {192, 168, 0, 1}),

    {ok, A1} = add(Node1, a, new()),
    {ok, A2} = add(Node2, a, A1),
    ?assertEqual(
        [Node2],
        to_list(A2)
    ),

    B1 = A1,
    B2 = merge(B1, A2),
    {ok, B3} = add(Node1, b, B2),

    ?assertEqual(
        [Node1],
        to_list(B3)
    ).

add_remove_test() ->
    Nodename = 'node1@127.0.0.1',
    Node = node_spec(Nodename),

    {ok, A} = add(Node, a, new()),

    ?assertEqual(
        [Node],
        to_list(A)
    ),

    {ok, A1} = remove(Nodename, a, A),

    ?assertEqual(
        [],
        to_list(A1)
    ).

concurrent_remove_update_test() ->
    Nodename = 'node1@127.0.0.1',
    Node1 = node_spec(Nodename),
    Node2 = node_spec(Nodename, {192, 168, 0, 1}),

    {ok, A} = add(Node1, a, new()),
    {ok, A1} = remove(Nodename, a, A),

    B = A,
    {ok, B1} = add(Node2, b, B),


    ?assertEqual(
        [],
        to_list(A1)
    ),
    ?assertEqual(
        [Node2],
        to_list(B1)
    ),
    %% Replicate to A
    ?assertEqual(
        [Node2],
        to_list(merge(A1, B1))
    ).


concurrent_updates_test() ->
    Nodename = 'node1@127.0.0.1',
    Node1 = node_spec(Nodename),
    Node2 = node_spec(Nodename, {192, 168, 0, 1}),

    {ok, A} = add(Node1, a, new()),
    {ok, B} = add(Node2, b, new()),

    %% Replicate to A
    ?assertEqual(
        [Node2],
        to_list(merge(A, B))
    ).


%% This fails on previous version of riak_dt_map
assoc_test() ->
    Nodename = 'node1@127.0.0.1',
    Node = node_spec(Nodename),

    {ok, A} = add(Node, a, new()),
    {ok, B} = add(Node, b, new()),
    {ok, B2} = remove(Nodename, b, B),

    C = A,

    {ok, C3} = update([{remove, Nodename}], c, C),

    ?assertEqual(
        merge(A, merge(B2, C3)),
        merge(merge(A, B2), C3)
    ),

    ?assertEqual(
        to_list(merge(merge(A, C3), B2)),
        to_list(merge(merge(A, B2), C3))
    ),

    ?assertEqual(
        merge(merge(A, C3), B2),
        merge(merge(A, B2), C3)
    ).

clock_test() ->
    Nodename = 'node1@127.0.0.1',
    Node1 = node_spec(Nodename),
    Node2 = node_spec(Nodename, {192, 168, 0, 1}),

    {ok, A} = update([{add, Node1}], a, new()),
    B = A,
    {ok, B2} = update([{add, Node2}], b, B),

    {ok, A2} = update([{remove, Nodename}], a, A),
    {error, {not_member, Nodename}} = update([{remove, Nodename}], a, A2),
    {ok, A4} = update([{add, Node2}], a, A2),
    AB = merge(A4, B2),

    %% LWW
    ?assertEqual([Node2], to_list(AB)).

remfield_test() ->
    Name = 'node1@127.0.0.1',
    Node1 = node_spec(Name),
    Node2 = node_spec(Name, {192, 168, 0, 1}),

    {ok, A} = update([{add, Node1}], a, new()),
    B = A,
    {ok, A2} = update([{remove, Name}], a, A),
    {error, {not_member, Name}} = update([{remove, Name}], a, A2),
    {ok, A4} = update([{add, Node2}], a, A2),
    AB = merge(A4, B),
    ?assertEqual([Node2], to_list(AB)).

%% Bug found by EQC, not dropping dots in merge when an element is
%% present in both Maos leads to removed items remaining after merge.
present_but_removed_test() ->
    Name = 'node1@127.0.0.1',
    Node1 = node_spec(Name),
    Node2 = node_spec(Name, {192, 168, 0, 1}),
    %% Add Z to A
    {ok, A} = update([{add, Node1}], a, new()),
    %% Replicate it to C so A has 'Z'->{a, 1}
    C = A,
    %% Remove Z from A
    {ok, A2} = update([{remove, Name}], a, A),
    %% Add Z to B, a new replica
    {ok, B} = update([{add, Node2}], b, new()),
    %%  Replicate B to A, so now A has a Z, the one with a Dot of
    %%  {b,1} and clock of [{a, 1}, {b, 1}]
    A3 = merge(B, A2),
    %% Remove the 'Z' from B replica
    {ok, B2} = update([{remove, Name}], b, B),
    %% Both C and A have a 'Z', but when they merge, there should be
    %% no 'Z' as C's has been removed by A and A's has been removed by
    %% C.
    Merged = lists:foldl(fun(Set, Acc) ->
                                 merge(Set, Acc) end,
                         %% the order matters, the two replicas that
                         %% have 'Z' need to merge first to provoke
                         %% the bug. You end up with 'Z' with two
                         %% dots, when really it should be removed.
                         A3,
                         [C, B2]),
    ?assertEqual([], to_list(Merged)).


%% A bug EQC found where dropping the dots in merge was not enough if
%% you then store the value with an empty clock (derp).
no_dots_left_test() ->
    Name = 'node1@127.0.0.1',
    Node1 = node_spec(Name),
    Node2 = node_spec(Name, {192, 168, 0, 1}),
    {ok, A} =  update([{add, Node1}], a, new()),
    {ok, B} =  update([{add, Node2}], b, new()),
    C = A, %% replicate A to empty C
    {ok, A2} = update([{remove, Name}], a, A),
    %% replicate B to A, now A has B's 'Z'
    A3 = merge(A2, B),
    %% Remove B's 'Z'
    {ok, B2} = update([{remove, Name}], b, B),
    %% Replicate C to B, now B has A's old 'Z'
    B3 = merge(B2, C),
    %% Merge everytyhing, without the fix You end up with 'Z' present,
    %% with no dots
    Merged = lists:foldl(fun(Set, Acc) ->
                                 merge(Set, Acc) end,
                         A3,
                         [B3, C]),
    ?assertEqual([], to_list(Merged)).

%% A reset-remove bug eqc found where dropping a superseded dot lost
%% field remove merge information the dropped dot contained, adding
%% the tombstone fixed this.
tombstone_remove_test() ->
    Name = 'node1@127.0.0.1',
    Node1 = node_spec(Name),
    Node2 = node_spec(Name, {192, 168, 0, 1}),
    Node3 = node_spec(Name, {192, 168, 0, 2}),

    A=B=new(),
    {ok, A1} = update([{add, Node1}], a, A),

    %% Replicate!
    B1 = merge(A1, B),
    ?assertEqual([Node1], to_list(B1)),

    {ok, A2} = update([{remove, Name}], a, A1),

    {ok, B2} = update([{add, Node2}], b, B1),

    %% Replicate
    A3 = merge(A2, B2),
    %% that remove of nodenamme from A means remove
    %% the Node1 A added to nodename
    ?assertEqual([Node2], to_list(A3)),

    {ok, B3} = update([{add, Node3}], b, B2),
    %% replicate to A
    A4 = merge(A3, B3),

    ?assertEqual([Node3], to_list(A4)),

    %% final values
    Final = merge(A4, B3),
    %% before adding the tombstone, the dropped dots were simply
    %% merged with the surviving field. When the second update to B
    %% was merged with A, that information contained in the superseded
    %% field in A at {b,1} was lost (since it was merged into the
    %% _VALUE_). This caused the [0] from A's first dot to
    %% resurface. By adding the tombstone, the superseded field merges
    %% it's tombstone with the surviving {b, 2} field so the remove
    %% information is preserved, even though the {b, 1} value is
    %% dropped. Pro-tip, don't alter the CRDTs' values in the merge!
    ?assertEqual(
        [Node3],
        to_list(Final)
    ).

%% This test is a regression test for a counter example found by eqc.
%% The previous version of riak_dt_map used the `dot' from the field
%% update/creation event as key in `merge_left/3'. Of course multiple
%% fields can be added/updated at the same time. This means they get
%% the same `dot'. When merging two replicas, it is possible that one
%% has removed one or more of the fields added at a particular `dot',
%% which meant a function clause error in `merge_left/3'. The
%% structure was wrong, it didn't take into account the possibility
%% that multiple fields could have the same `dot', when clearly, they
%% can. This test fails with `dot' as the key for a field in
%% `merge_left/3', but passes with the current structure, of
%% `{nodename(), dot()}' as key.
dot_key_test() ->
    Name1 = 'node1@127.0.0.1',
    Node1 = node_spec(Name1),
    Name2 = 'node2@127.0.0.1',
    Node2 = node_spec(Name2),

    {ok, A} = update([{add, Node1}, {add, Node2}], a, new()),
    B = A,
    {ok, A2} = update([{remove, Name1}], a, A),
    ?assertEqual([Node2], to_list(merge(B, A2))).

stat_test() ->
    Map = new(),
    Name1 = 'node1@127.0.0.1',
    Node1 = node_spec(Name1),
    Name2 = 'node2@127.0.0.1',
    Node2 = node_spec(Name2),


    {ok, Map1} = update([{add, Node1}], a1, Map),
    {ok, Map2} = update([{add, Node1}], a2, Map),
    {ok, Map3} = update([{add, Node1}], a3, Map),
    {ok, Map4} = update([{add, Node2}], a4, Map),

    Map5 = merge(merge(merge(merge(Map1, Map2), Map3), Map3), Map4),

    ?assertEqual([Node1, Node2], to_list(Map5)),

    ?assertEqual(
        [{actor_count, 0}, {member_count, 0}, {duplication, 0}, {deferred_length, 0}],
        stats(Map)
    ),
    ?assertEqual(4, stat(actor_count, Map5)),
    ?assertEqual(2, stat(member_count, Map5)),
    ?assertEqual(undefined, stat(waste_pct, Map5)),
    ?assertEqual(2, stat(duplication, Map5)),


    {ok, Map6} = update([{remove, Name2}], a4, Map5),
    Map7 = merge(Map5, Map6),
    ?assertEqual(4, stat(actor_count, Map7)),
    ?assertEqual(1, stat(member_count, Map7)).


equals_test() ->
    Name1 = 'node1@127.0.0.1',
    Node1 = node_spec(Name1),
    {ok, A} = update([{add, Node1}], a, new()),
    {ok, B} = update([{add, Node1}], b, new()),
    ?assert(not equal(A, B)),
    C = merge(A, B),
    D = merge(B, A),
    ?assert(equal(C, D)),
    ?assert(equal(A, A)).

-endif.

