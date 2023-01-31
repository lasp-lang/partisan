%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
%% Copyright (c) 2022 Alejandro M. Ramallo. All Rights Reserved.
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

%% -----------------------------------------------------------------------------
%% @doc Remote references are Partisan's representation for remote process
%% identifiers (`pid()'), registered names and references (`reference()').
%%
%% Distributed Erlang (disterl) will transform the representation of process
%% identifiers, registered names and references when they are sent to a remote
%% node. This is done to disambiguate between remote and local instances.
%% Because Partisan doesn't use disterl it needs to implement this same
%% disambiguation mechanism somehow. As disterl's implementation is done by the
%% BEAM internally and not API is exposed, this module is required to
%% achieve a similar result.
%%
%% == Representation ==
%% In cases where lots of references are stored in process state, `ets' and
%% specially where those are uses as keys, a binary format is preferable to the
%% tuple format in order to save memory and avoid copying the term every time a
%% message is send between processes (by leveraging off-heap binary storage).
%%
%% For this reason, this module implements two alternative representations:
%% <ul>
%% <li>references as binary URIs</li>
%% <li>references as tuples</li>
%% </ul>
%%
%% The representation to use is controlled by the configuration option
%% `remote_ref_as_uri`. If `true' this module will generate references as
%% binary URIs. Otherwise it will generate them as tuples.r
%%
%% === URI Representation ===
%%
%% ```
%% 1> partisan_remote_ref:from_term(self()).
%% <<"partisan:pid:nonode@nohost:0.1062.0">>
%% '''
%%
%% ==== URI Padding ====
%%
%% For those cases where the resulting references are smaller than 64 bytes (
%% and thus will be stored on the process heap) this module can pad the
%% generated binary URIs to 65 bytes, thus forcing them to be stored off-heap.
%% This is controlled with the configuration option `remote_ref_binary_padding'.
%%
%% ```
%% 1> partisan_config:set(remote_ref_binary_padding, false).
%% 2> partisan_remote_ref:from_term(self()).
%% <<"partisan:pid:nonode@nohost:0.1062.0">>
%% 3> partisan_config:set(remote_ref_binary_padding, true).
%% ok
%% 4> partisan_remote_ref:from_term(self()).
%% <<"partisan:pid:nonode@nohost:0.1062.0:"...>>
%% '''
%%
%% === Tuple Representation ===
%%
%% ```
%% 1> partisan_remote_ref:from_term(self()).
%% {partisan_remote_reference,
%%    nonode@nohost,
%%    {partisan_process_reference,"<0.1062.0>"}}
%% '''
%%
%% == Issues and TODOs ==
%% As opposed to erlang pid encodintg (`NEW_PID_EXT`) our current
%% representation cannot distinguished between identifiers from old (crashed)
%% nodes from a new one. So maybe we need to adopt the `NEW_PID_EXT' `Creation'
%% attribute.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_remote_ref).

-include("partisan_logger.hrl").
-include("partisan.hrl").

-type t()               ::  p() | r() | n().
-type uri()             ::  <<_:64, _:_*8>>.
-type p()               ::  tuple_ref(encoded_pid()) | uri().
-type r()               ::  tuple_ref(encoded_ref()) | uri().
-type n()               ::  tuple_ref(encoded_name()) | uri().
-type tuple_ref(T)      ::  {partisan_remote_ref, node(), T}.
-type target()          ::  encoded_pid() | encoded_ref() | encoded_name().
-type encoded_pid()     ::  {encoded_pid, list()}.
-type encoded_name()    ::  {encoded_name, list()}.
-type encoded_ref()     ::  {encoded_ref, list()}.


-export_type([t/0]).
-export_type([p/0]).
-export_type([r/0]).
-export_type([n/0]).
-export_type([encoded_pid/0]).
-export_type([encoded_name/0]).
-export_type([encoded_ref/0]).


-export([from_term/1]).
-export([from_term/2]).
-export([is_identical/2]).
-export([is_local/1]).
-export([is_local/2]).
-export([is_local_name/1]).
-export([is_local_pid/1]).
-export([is_local_reference/1]).
-export([is_name/1]).
-export([is_name/2]).
-export([is_pid/1]).
-export([is_reference/1]).
-export([is_type/1]).
-export([node/1]).
-export([nodestring/1]).
-export([target/1]).
-export([to_term/1]).


-compile({no_auto_import, [is_pid/1]}).
-compile({no_auto_import, [is_reference/1]}).
-compile({no_auto_import, [node/1]}).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec from_term(pid() | reference() | atom() | {atom(), node()}) ->
    t() | no_return().

from_term({Name, Node}) ->
    from_term(Name, Node);

from_term(Term)
when is_atom(Term); erlang:is_pid(Term); erlang:is_reference(Term) ->
    encode(Term, partisan:node()).


%% -----------------------------------------------------------------------------
%% @doc Takes an atom `Name' and a node `Node' and returns a
%% partisan remote reference.
%% @end
%% -----------------------------------------------------------------------------
-spec from_term(Name :: atom(), Node :: node()) ->
    n() | no_return().

from_term(Name, Node) when is_atom(Name), is_atom(Node) ->
    encode(Name, Node);

from_term(_, _) ->
    error(badarg).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec to_term(t()) ->
    pid() | reference() | atom() | no_return().

to_term(Ref) ->
    decode(Ref, term).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec target(Ref :: t()) -> target() | no_return().

target(Ref) ->
    decode(Ref, target).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node(Ref :: t()) -> node() | no_return().

node(<<"partisan:pid:", Rest/binary>>) ->
    get_node(Rest);

node(<<"partisan:ref:", Rest/binary>>) ->
    get_node(Rest);

node(<<"partisan:name:", Rest/binary>>) ->
    get_node(Rest);

node({partisan_remote_ref, Node, _}) ->
    Node;

node(_) ->
    error(badarg).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec nodestring(Ref :: t()) -> binary() | no_return().

nodestring(<<"partisan:pid:", Rest/binary>>) ->
    get_nodestring(Rest);

nodestring(<<"partisan:ref:", Rest/binary>>) ->
    get_nodestring(Rest);

nodestring(<<"partisan:name:", Rest/binary>>) ->
    get_nodestring(Rest);

nodestring({partisan_remote_ref, Node, _}) ->
    atom_to_binary(Node, utf8);

nodestring(_) ->
    error(badarg).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local(Ref :: t()) -> boolean() | no_return().

is_local(<<"partisan:pid:", Rest/binary>>) ->
    do_is_local(Rest);

is_local(<<"partisan:ref:", Rest/binary>>) ->
    do_is_local(Rest);

is_local(<<"partisan:name:", Rest/binary>>) ->
    do_is_local(Rest);

is_local({partisan_remote_ref, Node, _}) ->
    Node =:= partisan:node();

is_local(Arg) ->
    error({badarg, Arg}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local_pid(Ref :: t()) -> boolean() | no_return().

is_local_pid(<<"partisan:pid:", Rest/binary>>) ->
    do_is_local(Rest);

is_local_pid({partisan_remote_ref, Node, {encoded_pid, _}}) ->
    Node =:= partisan:node();

is_local_pid(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local_reference(Ref :: t()) -> boolean() | no_return().

is_local_reference(<<"partisan:ref:", Rest/binary>>) ->
    do_is_local(Rest);

is_local_reference({partisan_remote_ref, Node, {encoded_ref, _}}) ->
    Node =:= partisan:node();

is_local_reference(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local_name(Ref :: t()) -> boolean() | no_return().

is_local_name(<<"partisan:name:", Rest/binary>>) ->
    do_is_local(Rest);

is_local_name({partisan_remote_ref, Node, {encoded_name, _}}) ->
    Node =:= partisan:node();

is_local_name(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc Returns true if reference `Ref' is located in node `Node'.
%% @end
%% -----------------------------------------------------------------------------
-spec is_local(Ref :: t(), Node :: node()) ->
    boolean() | no_return().

is_local(<<"partisan:pid:", Rest/binary>>, Node) ->
    do_is_local(Rest, Node);

is_local(<<"partisan:ref:", Rest/binary>>, Node) ->
    do_is_local(Rest, Node);

is_local(<<"partisan:name:", Rest/binary>>, Node) ->
    do_is_local(Rest, Node);

is_local({partisan_remote_ref, Node, _}, OtherNode) ->
    Node =:= OtherNode;

is_local(_, _) ->
    error(badarg).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_type(any()) -> boolean().

is_type(Term) ->
    is_pid(Term) orelse
    is_reference(Term) orelse
    is_name(Term).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_pid(any()) -> boolean().

is_pid(<<"partisan:pid:", _/binary>>) ->
    true;

is_pid({partisan_remote_ref, _Node, Term}) ->
    is_pid(Term);

is_pid({encoded_pid, _}) ->
    true;

is_pid(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_reference(any()) -> boolean().

is_reference(<<"partisan:ref:", _/binary>>) ->
    true;

is_reference({partisan_remote_ref, _, Term}) ->
    is_reference(Term);

is_reference({encoded_ref, _}) ->
    true;

is_reference(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_name(any()) -> boolean().

is_name(<<"partisan:name:", _/binary>>) ->
    true;

is_name({partisan_remote_ref, _, Term}) ->
    is_name(Term);

is_name({encoded_name, _}) ->
    true;

is_name(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_name(Ref :: any(), Name :: atom()) -> boolean().

is_name(<<"partisan:name:", Rest/binary>>, Name) ->
    NameStr = atom_to_binary(Name, utf8),
    case binary:split(Rest, <<$:>>, [global]) of
        [_Node, NameStr] ->
            true;
        _ ->
            false
    end;

is_name({partisan_remote_ref, _, Term}, Name) ->
    is_name(Term, Name);

is_name({encoded_name, NameStr}, Name) ->
    NameStr == atom_to_list(Name);

is_name(_, _) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc Checks two refs for identity. Two refs are identical if the are
%% equal or if one is a process reference and the other one is a registered
%% name reference of said process.
%% In the latter case the function uses `erlang:whereis/1' which means the check
%% can fail if the process has died (and thus is no longer registered).
%% @end
%% -----------------------------------------------------------------------------
-spec is_identical(A :: t(), B :: t()) -> boolean().

is_identical(A, A) ->
    true;

is_identical(A, B) ->
    case node(A) == node(B) of
        true ->
            case {to_term(A), to_term(B)} of
                {Term, Term} ->
                    true;
                {Pid, Name} when erlang:is_pid(Pid) andalso is_atom(Name) ->
                    Pid == whereis(Name);
                _ ->
                    false
            end;
        false ->
            false
    end.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode(pid() | reference() | atom(), node()) -> t() | no_return().

encode(Term, Node) ->
    case partisan_config:get(remote_ref_as_uri, false) of
        true ->
            encode(Term, Node, uri);
        false ->
            encode(Term, Node, tuple)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode(pid() | reference() | atom(), node(), uri | tuple) ->
    t() | no_return().

encode(Pid, Node, Format) when erlang:is_pid(Pid) ->
    PidNode = erlang:node(Pid),
    PidStr0 = pid_to_list(Pid),
    PidStr =
        case PidNode == Node of
            true ->
                _ = maybe_register_pid(Pid, PidNode, local),
                PidStr0;
            false ->
                PidStr1 = to_local_pid_list(PidStr0),
                LocalPid = list_to_pid(PidStr1),
                _ = maybe_register_pid(LocalPid, PidNode, remote),
                PidStr1
        end,

    case Format of
        uri ->
            PidBin = untag(PidStr),
            Nodestr = atom_to_binary(Node, utf8),
            maybe_pad(<<"partisan:pid:", Nodestr/binary, $:, PidBin/binary>>);
        tuple ->
            Target = {encoded_pid, PidStr},
            {partisan_remote_ref, Node, Target}
    end;

encode(Ref, Node, uri) when erlang:is_reference(Ref) ->
    %% We do not support reference rewriting
    Node =:= partisan:node() orelse error(badarg),
    <<"#Ref", RefBin/binary>> = untag(ref_to_list(Ref)),
    Nodestring = atom_to_binary(Node, utf8),
    maybe_pad(<<"partisan:ref:", Nodestring/binary, $:, RefBin/binary>>);

encode(Ref, Node, tuple) when erlang:is_reference(Ref) ->
    %% We do not support reference rewriting
    Node =:= partisan:node() orelse error(badarg),
    Target = {encoded_ref, erlang:ref_to_list(Ref)},
    {partisan_remote_ref, Node, Target};

encode(Name, Node, uri) when is_atom(Name) ->
    NameBin = atom_to_binary(Name, utf8),
    Nodestring = atom_to_binary(Node, utf8),
    maybe_pad(<<"partisan:name:", Nodestring/binary, $:, NameBin/binary>>);

encode(Name, Node, tuple) when is_atom(Name) ->
    Target = {encoded_name, atom_to_list(Name)},
    {partisan_remote_ref, Node, Target}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec decode(t(), term | target) ->
    pid() | reference() | atom() | target() | no_return().

decode(<<"partisan:", Rest/binary>>, Mode) ->
    ThisNode = partisan:nodestring(),

    %% If padded then we will have 4 terms, so we match the first tree with cons
    %% and drop the tail.
    case binary:split(Rest, <<$:>>, [global]) of

        [<<"pid">>, _, Term | _] when Mode == target ->
            {encoded_pid, tag(Term)};

        [<<"ref">>, _, Term | _] when Mode == target ->
            {encoded_ref, "#Ref" ++ tag(Term)};

        [<<"name">>, _, Term | _] when Mode == target ->
            {encoded_name, binary_to_list(Term)};

        [<<"pid">>, Node, Term | _] when Node == ThisNode, Mode == term ->
            to_local_pid(tag(Term));

        [<<"ref">>, Node, Term | _] when Node == ThisNode, Mode == term ->
            list_to_ref("#Ref" ++ tag(Term));

        [<<"name">>, Node, Term | _] when Node == ThisNode, Mode == term ->
             binary_to_existing_atom(Term, utf8);

        _ ->
            error(badarg)
    end;

decode({partisan_remote_ref, Node, {Type, Value} = Target}, Mode) ->
    IsLocal = Node =:= partisan:node(),

    case Type of
        encoded_pid when IsLocal == true, Mode == term ->
            to_local_pid(Value);

        encoded_ref when IsLocal == true, Mode == term ->
            list_to_ref(Value);

        encoded_name when Mode == term ->
            %% Either local or remote
            list_to_existing_atom(Value);

        _ when Mode == target ->
            Target
    end;

decode({encoded_pid, _} = Target, target) ->
    Target;

decode({encoded_pid, Value}, term) ->
    list_to_pid(Value);

decode({encoded_ref, _} = Target, target) ->
    Target;

decode({encoded_ref, Value}, term) ->
    list_to_ref(Value);

decode({encoded_name, _} = Target, target) ->
    Target;

decode({encoded_name, Value}, term) ->
    list_to_existing_atom(Value).


%% @private
get_node(Bin) ->
    Nodestring = get_nodestring(Bin),
    binary_to_existing_atom(Nodestring, utf8).


%% @private
get_nodestring(Bin) ->
    case binary:split(Bin, <<$:>>) of
        [Node | _]  ->
            Node;
        _ ->
            error(badarg)
    end.


%% @private
do_is_local(Bin) ->
    do_is_local(Bin, partisan:nodestring()).


%% @private
do_is_local(Bin, Nodestring) ->
    Size = byte_size(Nodestring),
    case Bin of
        <<Nodestring:Size/binary, $:, _/binary>> ->
            true;
        _ ->
            false
    end.


%% @private
-spec untag(list()) -> binary().

untag(String0) ->
    String1 = string:replace(String0, "<", ""),
    iolist_to_binary(string:replace(String1, ">", "")).


%% @private
-spec tag(binary() | list()) -> list().

tag(Bin) when is_binary(Bin) ->
    tag(binary_to_list(Bin));

tag(String) when is_list(String) ->
    lists:append(["<", String, ">"]).


%% @private
maybe_pad(Bin) when byte_size(Bin) < 65 ->
    %% Erlang Heap binaries are small binaries, up to 64 bytes, and are stored
    %% directly on the process heap. They are copied when the process is
    %% garbage-collected and when they are sent as a message.
    %% Erlang Refc binaries (short for reference-counted binaries) - they
    %% consist of a ProcBin (stored on the process heap) and the binary object
    %% itself stored on the VM outside of all processes.
    %% TODO we need to support additional components so that apps can add
    %% metadata
    %% So, use $\31 as separator between URI and padding and pad with zeros
    %% string:pad(<<Bin/binary, $\31>>, 65, trailing, $0)
    %% when decoding, first split by $\31, dropping padding, then split by $:
    case partisan_config:get(remote_ref_binary_padding, false) of
        true ->
            iolist_to_binary(
                string:pad(<<Bin/binary, $:>>, 65, trailing, $\31)
            );
        false ->
            Bin
    end;

maybe_pad(Bin) ->
    Bin.


%% @private
to_local_pid_list(Value) ->
    case string:split(Value, ".", all) of
        ["<0", _, _] ->
            Value;

        [_, B, C] ->
            "<0." ++ B ++ "." ++ C;

        _ ->
            error(badarg)
    end.


%% @private
to_local_pid(Value) ->
    list_to_pid(to_local_pid_list(Value)).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
maybe_register_pid(Pid, Node, Mode) ->
    case partisan_config:get(register_pid_for_encoding, false) of
        true when Mode == local ->
            register_local_pid(Pid);
        true when Mode == remote ->
            register_remote_pid(Pid, Node);
        false ->
            false
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec register_local_pid(pid()) -> true.

register_local_pid(Pid) ->
    %% This is super dangerous.
    %% This code was in partisan_util in previous versions
    Unique = erlang:unique_integer([monotonic, positive]),

    Name = case process_info(Pid, registered_name) of
        {registered_name, Name0} ->
            ?LOG_DEBUG("unregistering pid: ~p name: ~p", [Pid, Name0]),
            %% TODO: Race condition on unregister/register.
            unregister(Name0),
            Name0;
        [] ->
            list_to_atom("partisan_registered_name_" ++ integer_to_list(Unique))
    end,

    ?LOG_DEBUG("registering pid: ~p name: ~p", [Pid, Name]),

    register(Name, Pid).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
register_remote_pid(Pid, Node) ->
    %% This is even more super dangerous.
    %% This code was in partisan_util in previous versions
    Unique = erlang:unique_integer([monotonic, positive]),
    NewName = "partisan_registered_name_" ++ integer_to_list(Unique),

    Register = fun() ->
        Name = case process_info(Pid, registered_name) of
            {registered_name, Name0} ->
                ?LOG_DEBUG("unregistering pid: ~p name: ~p", [Pid, Name0]),
                %% TODO: Race condition on unregister/register.
                unregister(Name0),
                Name0;
            [] ->
                list_to_atom(NewName)
        end,
        ?LOG_DEBUG(
            "registering pid: ~p name: ~p at node: ~p",
            [Pid, NewName, Node]
        ),
        erlang:register(Name, Pid)
    end,

    %% TODO: Race here unless we wait.
    _ = rpc:call(Node, erlang, spawn, [Register]),
    true.


%% =============================================================================
%% EUNIT TESTS
%% =============================================================================



-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

init_test() ->
    %% A hack to resolve node name
    partisan_config:init(),
    partisan_config:set(name, 'test@127.0.0.1'),
    partisan_config:set(nodestring, <<"test@127.0.0.1">>),
    ok.


local_pid_test() ->
    partisan_config:set(remote_ref_as_uri, false),
    Ref = from_term(self()),
    ?assert(is_pid(Ref)),
    ?assert(not is_reference(Ref)),
    ?assert(not is_name(Ref)),
    ?assert(is_type(Ref)),
    ?assert(is_local(Ref)),
    ?assertEqual(partisan:node(), ?MODULE:node(Ref)),
    ?assertEqual(partisan:nodestring(), ?MODULE:nodestring(Ref)),
    ?assertEqual(
        self(),
        to_term(Ref)
    ),
    ?assertEqual(
        {encoded_pid, pid_to_list(self())},
        target(Ref)
    ),

    partisan_config:set(remote_ref_as_uri, true),
    UriRef = from_term(self()),
    ?assert(is_pid(UriRef)),
    ?assert(not is_reference(UriRef)),
    ?assert(not is_name(UriRef)),
    ?assert(is_type(UriRef)),
    ?assert(is_local(UriRef)),
    ?assertEqual(partisan:node(), ?MODULE:node(UriRef)),
    ?assertEqual(partisan:nodestring(), ?MODULE:nodestring(UriRef)),
    ?assertEqual(
        self(),
        to_term(UriRef)
    ),
    ?assertEqual(
        {encoded_pid, pid_to_list(self())},
        target(UriRef)
    ).


local_name_test() ->
    partisan_config:set(remote_ref_as_uri, false),
    Ref = from_term(foo),
    ?assert(not is_pid(Ref)),
    ?assert(not is_reference(Ref)),
    ?assert(is_name(Ref)),
    ?assert(is_type(Ref)),
    ?assert(is_local(Ref)),
    ?assertEqual(partisan:node(), ?MODULE:node(Ref)),
    ?assertEqual(partisan:nodestring(), ?MODULE:nodestring(Ref)),
    ?assertEqual(
        foo,
        to_term(Ref)
    ),
    ?assertEqual(
        {encoded_name, "foo"},
        target(Ref)
    ),

    partisan_config:set(remote_ref_as_uri, true),
    UriRef = from_term(foo),
    ?assert(not is_pid(UriRef)),
    ?assert(not is_reference(UriRef)),
    ?assert(is_name(UriRef)),
    ?assert(is_type(UriRef)),
    ?assert(is_local(UriRef)),
    ?assertEqual(partisan:node(), ?MODULE:node(UriRef)),
    ?assertEqual(partisan:nodestring(), ?MODULE:nodestring(UriRef)),
    ?assertEqual(
        foo,
        to_term(UriRef)
    ),
    ?assertEqual(
        {encoded_name, "foo"},
        target(UriRef)
    ).

local_ref_test() ->
    ERef = make_ref(),
    partisan_config:set(remote_ref_as_uri, false),
    Ref = from_term(ERef),
    ?assert(not is_pid(Ref)),
    ?assert(is_reference(Ref)),
    ?assert(not is_name(Ref)),
    ?assert(is_type(Ref)),
    ?assert(is_local(Ref)),
    ?assertEqual(partisan:node(), ?MODULE:node(Ref)),
    ?assertEqual(partisan:nodestring(), ?MODULE:nodestring(Ref)),
    ?assertEqual(
        ERef,
        to_term(Ref)
    ),

    partisan_config:set(remote_ref_as_uri, true),
    UriRef = from_term(ERef),
    ?assert(not is_pid(UriRef)),
    ?assert(is_reference(UriRef)),
    ?assert(not is_name(UriRef)),
    ?assert(is_type(UriRef)),
    ?assert(is_local(UriRef)),
    ?assertEqual(partisan:node(), ?MODULE:node(UriRef)),
    ?assertEqual(partisan:nodestring(), ?MODULE:nodestring(UriRef)),
    ?assertEqual(
        ERef,
        to_term(UriRef)
    ).


remote_name_test() ->
    Ref = from_term(foo, 'othernode@127.0.0.1'),
    ?assertEqual(
        true,
        is_type(Ref)
    ),
    ?assertEqual(
        true,
        is_name(Ref)
    ).

from_term_badarg_test() ->
    ?assertError(
        badarg,
        from_term(self(), 'othernode@127.0.0.1')
    ),

    ?assertError(
        badarg,
        from_term(erlang:make_ref(), 'othernode@127.0.0.1')
    ).


non_local_target_test() ->
    Ref =  <<"partisan:pid:othernode@127.0.0.1:0.800.0">>,
    ?assertError(
        badarg,
        to_term(Ref)
    ),
    ?assertEqual(
        {encoded_pid, "<0.800.0>"},
        target(Ref)
    ).


identical_test() ->
    true = erlang:register(foo, self()),

    A = from_term(self()),
    B = from_term(foo),

    ?assert(
        is_identical(A, B)
    ),
    ?assert(
        is_identical(A, A)
    ),
    ?assert(
        is_identical(B, B)
    ),
    ?assertEqual(
        false,
        is_identical(A, from_term(bar))
    ).


is_name_test() ->
    ?assertEqual(
        true,
        is_name(from_term(foo), foo)
    ),
    ?assertEqual(
        false,
        is_name(from_term(foo), bar)
    ),
    ?assertEqual(
        false,
        is_name(from_term(self()), bar)
    ).


-endif.