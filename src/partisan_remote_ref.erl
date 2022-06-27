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
%% achieve the same result.
%%
%% == Representation ==
%% This module provide two representation formats for remote identifiers (in
%% general, "references"): (i) a binary URI and; (ii) a tuple.
%%
%% The functions in this module will check which format to use by reading the
%% configuration parameter `remote_ref_as_uri'. If `true' they will return an
%% URI representation. Otherwise they will return a tuple representation.
%%
%% === URI Representation ===
%%
%% ==== URI Padding ====
%%
%% === Tuple Representation ===
%%
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_remote_ref).

-include("partisan.hrl").

-type t()               ::  p() | r() | n().
-type p()               ::  remote_ref(process_ref()) | uri().
-type r()               ::  remote_ref(encoded_ref()) | uri().
-type n()               ::  remote_ref(registered_name_ref()) | uri().
-type uri()             ::  <<_:64, _:_*8>>.
-type tuple_ref()       ::  remote_ref()
                            | process_ref()
                            | encoded_ref()
                            | registered_name_ref().

-export_type([t/0]).
-export_type([p/0]).
-export_type([r/0]).
-export_type([n/0]).
-export_type([uri/0]).


-export([from_term/1]).
-export([from_term/2]).
-export([is_local/1]).
-export([is_local/2]).
-export([is_name/1]).
-export([is_pid/1]).
-export([is_reference/1]).
-export([is_type/1]).
-export([node/1]).
-export([nodestring/1]).
-export([target/1]).
-export([to_term/1]).


-compile({no_auto_import, [is_pid/1]}).
-compile({no_auto_import, [is_reference/1]}).


%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec from_term(pid() | reference() | atom() | {atom(), node()}) ->
    uri() | remote_ref() | no_return().

from_term({Name, Node}) when is_atom(Name); is_atom(Node) ->
    from_term(Name, Node);

from_term(Term) ->
    case partisan_config:get(remote_ref_as_uri, false) of
        true ->
            encode_as_uri(Term);
        false ->
            encode_as_tuple(Term)
    end.


%% -----------------------------------------------------------------------------
%% @doc Takes a local Erlang identifier `Term' and a node `Node' and returns a
%% partisan remote reference.
%% @end
%% -----------------------------------------------------------------------------
-spec from_term(Term :: pid() | reference() | atom(), Node :: node()) ->
    uri() | remote_ref() | no_return().

from_term(Term, Node) ->
    case partisan_config:get(remote_ref_as_uri, false) of
        true ->
            encode_as_uri(Term, Node);
        false ->
            encode_as_tuple(Term, Node)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec to_term(uri() | tuple_ref()) ->
    pid() | reference() | atom() | no_return().

to_term(RemoteRef) ->
    decode(RemoteRef, term).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec target(uri() | tuple_ref()) ->
    pid() | reference() | atom() | no_return().

target(RemoteRef) ->
    decode(RemoteRef, target).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node(Bin :: binary()) -> node() | no_return().

node(<<"partisan:pid:", Rest/binary>>) ->
    get_node(Rest);

node(<<"partisan:ref:", Rest/binary>>) ->
    get_node(Rest);

node(<<"partisan:name:", Rest/binary>>) ->
    get_node(Rest);

node({partisan_remote_reference, Node, _}) ->
    Node;

node(_) ->
    error(badarg).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec nodestring(Bin :: binary()) -> binary() | no_return().

nodestring(<<"partisan:pid:", Rest/binary>>) ->
    get_nodestring(Rest);

nodestring(<<"partisan:ref:", Rest/binary>>) ->
    get_nodestring(Rest);

nodestring(<<"partisan:name:", Rest/binary>>) ->
    get_nodestring(Rest);

nodestring({partisan_remote_reference, Node, _}) ->
    atom_to_binary(Node, utf8);

nodestring(_) ->
    error(badarg).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local(binary() | tuple()) -> boolean() | no_return().

is_local(<<"partisan:pid:", Rest/binary>>) ->
    do_is_local(Rest);

is_local(<<"partisan:ref:", Rest/binary>>) ->
    do_is_local(Rest);

is_local(<<"partisan:name:", Rest/binary>>) ->
    do_is_local(Rest);

is_local({partisan_remote_reference, Node, _}) ->
    Node =:= partisan:node();

is_local(_) ->
    error(badarg).


%% -----------------------------------------------------------------------------
%% @doc Returns true if reference `Ref' is located in node `Node'.
%% @end
%% -----------------------------------------------------------------------------
-spec is_local(Ref :: binary() | tuple(), Node :: node()) ->
    boolean() | no_return().

is_local(<<"partisan:pid:", Rest/binary>>, Node) ->
    do_is_local(Rest, Node);

is_local(<<"partisan:ref:", Rest/binary>>, Node) ->
    do_is_local(Rest, Node);

is_local(<<"partisan:name:", Rest/binary>>, Node) ->
    do_is_local(Rest, Node);

is_local({partisan_remote_reference, Node, _}, OtherNode) ->
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

is_pid({partisan_remote_reference, _, Term}) ->
    is_pid(Term);

is_pid({partisan_process_reference, _}) ->
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

is_reference({partisan_remote_reference, _, Term}) ->
    is_reference(Term);

is_reference({partisan_encoded_reference, _}) ->
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

is_name({partisan_remote_reference, _, Term}) ->
    is_name(Term);

is_name({partisan_registered_name_reference, _}) ->
    true;

is_name(_) ->
    false.


%% =============================================================================
%% PRIVATE
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode_as_uri(pid() | reference() | atom()) -> uri() | no_return().

encode_as_uri(Term) ->
    encode_as_uri(Term, partisan:nodestring()).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode_as_uri(pid() | reference() | atom(), node() | binary()) ->
    uri() | no_return().

encode_as_uri(Term, Node) when is_atom(Node) ->
    encode_as_uri(Term, atom_to_binary(Node, utf8));

encode_as_uri(Pid, Nodestring) when erlang:is_pid(Pid) ->
    PidBin = untag(pid_to_list(Pid)),
    maybe_pad(<<"partisan:pid:", Nodestring/binary, $:, PidBin/binary>>);

encode_as_uri(Ref, Nodestring) when erlang:is_reference(Ref) ->
    <<"#Ref", RefBin/binary>> = untag(ref_to_list(Ref)),
    maybe_pad(<<"partisan:ref:", Nodestring/binary, $:, RefBin/binary>>);

encode_as_uri(Name, Nodestring) when is_atom(Name) ->
    NameBin = atom_to_binary(Name, utf8),
    maybe_pad(<<"partisan:name:", Nodestring/binary, $:, NameBin/binary>>).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode_as_tuple(pid() | reference() | atom()) ->
    remote_ref() | no_return().

encode_as_tuple(Term) ->
    encode_as_tuple(Term, partisan:node()).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode_as_tuple(pid() | reference() | atom(), node()) ->
    remote_ref() | no_return().

encode_as_tuple(Pid, Node) when erlang:is_pid(Pid) ->
    Encoded = {partisan_process_reference, pid_to_list(Pid)},
    {partisan_remote_reference, Node, Encoded};

encode_as_tuple(Ref, Node) when erlang:is_reference(Ref) ->
    Encoded = {partisan_encoded_reference, erlang:ref_to_list(Ref)},
    {partisan_remote_reference, Node, Encoded};

encode_as_tuple(Name, Node) when is_atom(Name) ->
    Encoded = {partisan_registered_name_reference, atom_to_list(Name)},
    {partisan_remote_reference, Node, Encoded}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec decode(uri() | tuple_ref(), term | target) ->
    pid() | reference() | atom() | no_return().

decode(<<"partisan:", Rest/binary>>, Mode) ->
    ThisNode = partisan:nodestring(),

    %% If padded then we will have 4 terms, so we match the first tree with cons
    %% and drop the tail.
    case binary:split(Rest, <<$:>>, [global]) of
        [<<"pid">>, Node, Term | _] when Node == ThisNode, Mode == term ->
            to_local_pid(tag(Term));

        [<<"pid">>, Node, Term | _] when Node == ThisNode, Mode == target ->
            {partisan_process_reference, tag(Term)};

        [<<"ref">>, Node, Term | _] when Node == ThisNode, Mode == term ->
            list_to_ref("#Ref" ++ tag(Term));

        [<<"ref">>, Node, Term | _] when Node == ThisNode, Mode == target ->
            {partisan_encoded_reference, "#Ref" ++ tag(Term)};

        [<<"name">>, Node, Term | _] when Node == ThisNode, Mode == term ->
             binary_to_existing_atom(Term, utf8);

        [<<"name">>, Node, Term | _] when Node == ThisNode, Mode == target ->
            {partisan_registered_name_reference, Term};

        _ ->
            error(badarg)
    end;

decode({partisan_remote_reference, Node, {Type, Value} = Target}, Mode) ->
    IsLocal = Node =:= partisan:node(),

    case Type of
        partisan_process_reference when IsLocal == true, Mode == term ->
            to_local_pid(Value);

        partisan_encoded_reference when IsLocal == true, Mode == term ->
            list_to_ref(Value);

        partisan_registered_name_reference when Mode == term ->
            %% Either local or remote
            list_to_existing_atom(Value);

        _ when Mode == target ->
            Target
    end;

decode({partisan_process_reference, _} = Target, target) ->
    Target;

decode({partisan_process_reference, Value}, term) ->
    list_to_pid(Value);

decode({partisan_encoded_reference, _} = Target, target) ->
    Target;

decode({partisan_encoded_reference, Value}, term) ->
    list_to_ref(Value);

decode({partisan_registered_name_reference, _} = Target, target) ->
    Target;

decode({partisan_registered_name_reference, Value}, term) ->
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
to_local_pid(Value) ->
    case string:split(Value, ".", all) of
        ["<0", _, _] ->
            list_to_pid(Value);

        [_, B, C] ->
            list_to_pid("<0." ++ B ++ "." ++ C);

        _ ->
            error(badarg)
    end.





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
        {partisan_process_reference, pid_to_list(self())}
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
        {partisan_process_reference, pid_to_list(self())}
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



-endif.