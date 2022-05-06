-module(partisan_remote_ref).


-export([decode/1]).
-export([encode/1]).
-export([is_local/1]).
-export([is_name/1]).
-export([is_pid/1]).
-export([is_reference/1]).
-export([node/1]).


%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode(pid() | reference() | atom()) -> binary() | no_return().

encode(Pid) when erlang:is_pid(Pid) ->
    Node = atom_to_binary(partisan:node()),
    PidBin = encode_term(pid_to_list(Pid)),
    <<"partisan:pid:", Node/binary, $:, PidBin/binary>>;

encode(Ref) when erlang:is_reference(Ref) ->
    Node = atom_to_binary(partisan:node()),
    <<"#Ref", RefBin/binary>> = encode_term(ref_to_list(Ref)),
    <<"partisan:ref:", Node/binary, $:, RefBin/binary>>;

encode(Name) when is_atom(Name) ->
    Node = atom_to_binary(partisan:node(), utf8),
    NameBin = atom_to_binary(Name, utf8),
    <<"partisan:name:", Node/binary, $:, NameBin/binary>>.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec decode(binary()) -> pid() | reference() | atom() | no_return().

decode(<<"partisan:", Rest/binary>>) ->
    ThisNode = nodestring(),

    %% If padded then we will have 4 terms, so we match the first tree with cons
    %% and drop the tail.
    case binary:split(Rest, <<$:>>, [global]) of
        [<<"pid">>, Node, Term | _] when Node == ThisNode ->
            list_to_pid(decode_term(Term));

        [<<"ref">>, Node, Term | _] when Node == ThisNode ->
            list_to_ref("#Ref" ++ decode_term(Term));

        [<<"name">>, Node, Term | _] when Node == ThisNode ->
            binary_to_existing_atom(Term, utf8);

        _ ->
            error(badarg)
    end.


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

node(_) ->
    error(badarg).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local(binary()) -> boolean() | no_return().

is_local(<<"partisan:pid:", Rest/binary>>) ->
    do_is_local(Rest);

is_local(<<"partisan:ref:", Rest/binary>>) ->
    do_is_local(Rest);

is_local(<<"partisan:name:", Rest/binary>>) ->
    do_is_local(Rest);

is_local(_) ->
    error(badarg).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_pid(Bin :: binary()) -> boolean().

is_pid(<<"partisan:pid:", _/binary>>) ->
    true;

is_pid(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_reference(Bin :: binary()) -> boolean().

is_reference(<<"partisan:ref:", _/binary>>) ->
    true;

is_reference(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_name(Bin :: binary()) -> boolean().

is_name(<<"partisan:name:", _/binary>>) ->
    true;

is_name(_) ->
    false.


%% =============================================================================
%% PRIVATE
%% =============================================================================



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
    Node = nodestring(),
    Size = byte_size(Node),
    case Bin of
        <<Node:Size/binary, $:, _/binary>> ->
            true;
        _ ->
            false
    end.


%% @private
nodestring() ->
    case partisan_config:get(nodestring, undefined) of
        undefined ->
            String = atom_to_binary(partisan:node(), utf8),
            _ = partisan_config:set(nodestring, String),
            String;
        Value ->
            Value
    end.


%% @private
-spec encode_term(list()) -> binary().

encode_term(String0) ->
    String1 = string:replace(String0, "<", ""),
    iolist_to_binary(string:replace(String1, ">", "")).


%% @private
-spec decode_term(binary() | list()) -> list().

decode_term(Bin) when is_binary(Bin) ->
    decode_term(binary_to_list(Bin));

decode_term(String) when is_list(String) ->
    lists:append(["<", String, ">"]).


