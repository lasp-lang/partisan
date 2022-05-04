-module(partisan_remote_ref).


-export([encode/1]).
-export([decode/1]).
-export([is_local/1]).


%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode(pid() | reference() | atom()) -> binary() | no_return().

encode(Pid) when is_pid(Pid) ->
    Node = atom_to_binary(partisan:node()),
    PidBin = encode_term(pid_to_list(Pid)),
    <<"partisan", $:, Node/binary, $:, "Pid", PidBin/binary>>;

encode(Ref) when is_reference(Ref) ->
    Node = atom_to_binary(partisan:node()),
    <<"#Ref", RefBin/binary>> = encode_term(ref_to_list(Ref)),
    <<"partisan", $:, Node/binary, $:, "Ref", RefBin/binary>>;

encode(Name) when is_atom(Name) ->
    Node = atom_to_binary(partisan:node(), utf8),
    NameBin = atom_to_binary(Name, utf8),
    <<"partisan", $:, Node/binary, $:, NameBin/binary>>.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec decode(binary()) -> pid() | reference() | atom() | no_return().

decode(<<"partisan:", Rest/binary>>) ->
    ThisNode = nodestring(),

    %% If padded then we will have 3 terms, so we match with cons
    %% and drop the tail.
    case binary:split(Rest, <<$:>>) of
        [Node, <<"Pid", Term/binary>> | _] when Node == ThisNode ->
            list_to_pid(decode_term(Term));

        [Node, <<"Ref", Term/binary>> | _] when Node == ThisNode ->
            list_to_ref("#Ref" ++ decode_term(Term));

        [Node, Bin | _] when Node == ThisNode ->
            binary_to_existing_atom(Bin, utf8);

        _ ->
            error(badarg)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local(binary()) -> boolean() | no_return().

is_local(<<"partisan:", Rest/binary>>) ->
    Node = nodestring(),
    Size = byte_size(Node),
    case Rest of
        <<Node:Size/binary, $:, _/binary>> ->
            true;
        _ ->
            false
    end;

is_local(_) ->
    error(badarg).



%% =============================================================================
%% PRIVATE
%% =============================================================================



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
    String1 = string:replace(String0, "<", "("),
    iolist_to_binary(string:replace(String1, ">", ")")).


%% @private
-spec decode_term(binary() | list()) -> list().

decode_term(Bin) when is_binary(Bin) ->
    decode_term(binary_to_list(Bin));

decode_term(String0) when is_list(String0)->
    String1 = string:replace(String0, "(", "<"),
    lists:flatten(string:replace(String1, ")", ">")).


