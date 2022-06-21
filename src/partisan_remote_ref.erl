-module(partisan_remote_ref).

-include("partisan.hrl").

-type t()               :: p() | r() | n().
-type p()               :: remote_ref(process_ref()) | binary().
-type r()               :: remote_ref(encoded_ref()) | binary().
-type n()               :: remote_ref(registered_name_ref()) | binary().

-export_type([t/0]).
-export_type([p/0]).
-export_type([r/0]).
-export_type([n/0]).


-export([from_term/1]).
-export([from_term/2]).
-export([is_local/1]).
-export([is_name/1]).
-export([is_pid/1]).
-export([is_reference/1]).
-export([is_type/1]).
-export([node/1]).
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
-spec from_term(pid() | reference() | atom()) -> binary() | no_return().

from_term(Term) ->
    case partisan_config:get(remote_ref_as_uri, false) of
        true ->
            encode_as_uri(Term);
        false ->
            encode_as_tuple(Term)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec from_term(pid() | reference() | atom(), node()) -> binary() | no_return().

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
-spec to_term(binary()) -> pid() | reference() | atom() | no_return().

to_term(RemoteRef) ->
    decode(RemoteRef).


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

is_pid({partisan_remote_reference, _, {Type, _}}) ->
    Type =:= partisan_process_reference;

is_pid(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_reference(any()) -> boolean().

is_reference(<<"partisan:ref:", _/binary>>) ->
    true;

is_reference({partisan_remote_reference, _, {Type, _}}) ->
    Type =:= partisan_encoded_reference;

is_reference(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_name(any()) -> boolean().

is_name(<<"partisan:name:", _/binary>>) ->
    true;

is_name({partisan_remote_reference, _, {Type, _}}) ->
    Type =:= partisan_registered_name_reference;

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
-spec encode_as_uri(pid() | reference() | atom()) -> binary() | no_return().

encode_as_uri(Term) ->
    encode_as_uri(Term, partisan:node()).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode_as_uri(pid() | reference() | atom(), node()) ->
    binary() | no_return().

encode_as_uri(Pid, Node) when erlang:is_pid(Pid) ->
    Node = atom_to_binary(Node),
    PidBin = untag(pid_to_list(Pid)),
    maybe_pad(<<"partisan:pid:", Node/binary, $:, PidBin/binary>>);

encode_as_uri(Ref, Node) when erlang:is_reference(Ref) ->
    Node = atom_to_binary(Node),
    <<"#Ref", RefBin/binary>> = untag(ref_to_list(Ref)),
    maybe_pad(<<"partisan:ref:", Node/binary, $:, RefBin/binary>>);

encode_as_uri(Name, Node) when is_atom(Name) ->
    Node = atom_to_binary(Node, utf8),
    NameBin = atom_to_binary(Name, utf8),
    maybe_pad(<<"partisan:name:", Node/binary, $:, NameBin/binary>>).


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
    Encoded = {partisan_encoded_reference, atom_to_list(Name)},
    {partisan_registered_name_reference, Node, Encoded}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec decode(binary() | remote_ref()) ->
    pid() | reference() | atom() | no_return().

decode(<<"partisan:", Rest/binary>>) ->
    ThisNode = partisan:nodestring(),

    %% If padded then we will have 4 terms, so we match the first tree with cons
    %% and drop the tail.
    case binary:split(Rest, <<$:>>, [global]) of
        [<<"pid">>, Node, Term | _] when Node == ThisNode ->
            list_to_pid(tag(Term));

        [<<"ref">>, Node, Term | _] when Node == ThisNode ->
            list_to_ref("#Ref" ++ tag(Term));

        [<<"name">>, Node, Term | _] when Node == ThisNode ->
            binary_to_existing_atom(Term, utf8);

        _ ->
            error(badarg)
    end;

decode({partisan_remote_reference, Node, {Type, Value}}) ->
    Node =:= partisan:node() orelse error(badarg),
    case Type of
        partisan_remote_reference -> list_to_pid(Value);
        partisan_encoded_reference -> erlang:ref_to_list(Value);
        partisan_registered_name_reference -> list_to_atom(Value)
    end.



%% @private
get_node(Bin) ->
    case binary:split(Bin, <<$:>>) of
        [Node | _]  ->
            binary_to_existing_atom(Node, utf8);
        _ ->
            error(badarg)
    end.


%% @private
do_is_local(Bin) ->
    Nodestring = partisan:nodestring(),
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
