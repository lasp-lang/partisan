-module(partisan_hbbft_worker).

-behaviour(gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-export([start_link/6, submit_transaction/2, start_on_demand/1, get_blocks/1, get_status/1, get_buf/1, stop/1, terminate/2]).
-export([verify_chain/2, block_transactions/1]).

-export([sync/2, fetch_from/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

-record(block, {
          prev_hash :: binary(),
          transactions :: [binary()],
          signature :: binary()
         }).

-record(state, {
          n :: non_neg_integer(),
          id :: non_neg_integer(),
          hbbft :: hbbft:hbbft_data(),
          blocks :: [#block{}],
          tempblock :: undefined | #block{},
          sk :: tpke_privkey:privkey(),
          ssk :: tpke_privkey:privkey_serialized(),
          to_serialize = false :: boolean(),
          defer_queue = []
         }).

start_link(N, F, ID, SK, BatchSize, ToSerialize) ->
    gen_server:start_link({global, name(ID)}, ?MODULE, [N, F, ID, SK, BatchSize, ToSerialize], []).

stop(Pid) ->
    gen_server:stop(Pid).

submit_transaction(Msg, Pid) ->
    gen_server:call(Pid, {submit_txn, Msg}, infinity).

sync(Pid, Target) ->
    gen_server:call(Pid, {sync, Target}, infinity).

fetch_from(Pid, Highest) ->
    gen_server:call(Pid, {fetch_from, Highest}, infinity).

start_on_demand(Pid) ->
    gen_server:call(Pid, start_on_demand, infinity).

get_blocks(Pid) ->
    gen_server:call(Pid, get_blocks, infinity).

get_status(Pid) ->
    gen_server:call(Pid, get_status, infinity).

get_buf(Pid) ->
    gen_server:call(Pid, get_buf, infinity).

verify_chain([], _) ->
    true;
verify_chain([G], PubKey) ->
    ?LOG_INFO(#{description => "verifying genesis block~n"}),
    %% genesis block has no prev hash
    case G#block.prev_hash == <<>> of
        true ->
            %% genesis block should have a valid signature
            HM = tpke_pubkey:hash_message(PubKey, term_to_binary(G#block{signature= <<>>})),
            Signature = tpke_pubkey:deserialize_element(PubKey, G#block.signature),
            tpke_pubkey:verify_signature(PubKey, Signature, HM);
        false ->
            ?LOG_INFO(#{description => "no genesis block~n"}),
            false
    end;
verify_chain(Chain, PubKey) ->
    ?LOG_INFO("Chain verification depth ~p~n", [length(Chain)]),
    case verify_block_fit(Chain, PubKey) of
        true -> verify_chain(tl(Chain), PubKey);
        false ->
            ?LOG_INFO(#{description => "bad signature~n"}),
            false
    end.

verify_block_fit([B], _) when B#block.prev_hash == <<>> -> true;
verify_block_fit([A, B | _], PubKey) ->
    %% A should have the the prev_hash of B
    case A#block.prev_hash == hash_block(B) of
        true ->
            %% A should have a valid signature
            HM = tpke_pubkey:hash_message(PubKey, term_to_binary(A#block{signature= <<>>})),
            Signature = tpke_pubkey:deserialize_element(PubKey, A#block.signature),
            case tpke_pubkey:verify_signature(PubKey, Signature, HM) of
                true ->
                    true;
                false ->
                    ?LOG_INFO(#{description => "bad signature~n"}),
                    false
            end;
        false ->
            ?LOG_INFO("parent hash mismatch ~p ~p~n", [A#block.prev_hash, hash_block(B)]),
            false
    end.

block_transactions(Block) ->
    Block#block.transactions.

init([N, F, ID, SK, BatchSize, ToSerialize]) ->
    %% deserialize the secret key once
    DSK = tpke_privkey:deserialize(SK),
    %% init hbbft
    HBBFT = hbbft:init(DSK, N, F, ID, BatchSize, infinity),
    %% store the serialized state and serialized SK
    {ok, #state{hbbft=HBBFT, blocks=[], id=ID, n=N, sk=DSK, ssk=SK, to_serialize=ToSerialize}}.

handle_call(start_on_demand, _From, State = #state{hbbft=HBBFT, sk=SK}) ->
    NewState = dispatch(hbbft:start_on_demand(maybe_deserialize_hbbft(HBBFT, SK)), undefined, State),
    {reply, ok, NewState};
handle_call({submit_txn, Txn}, _From, State = #state{hbbft=HBBFT, sk=SK}) ->
    NewState = dispatch(hbbft:input(maybe_deserialize_hbbft(HBBFT, SK), Txn), undefined, State),
    {reply, ok, NewState};
handle_call({sync, Target}, _From, #state{blocks = Blocks} = State) ->
    case self() of
        Target ->
            %% deadlock avoidance, also handled at the prop level
            {reply, ok, State};
        _ ->
            %% todo check for disjoint chain if there is ever a
            %% possiblity of bad sync (I don't think there is right now)
            {ok, FetchedBlocks} = fetch_from(Target, hd(Blocks)),
            Blocks1 = lists:append(FetchedBlocks, Blocks),
            {reply, ok, State#state{blocks = Blocks1}}
    end;
handle_call({fetch_from, Highest}, _From, #state{blocks = Blocks} = State) ->
    case lists:member(Highest, Blocks) of
        true ->
            Fetched = lists:takewhile(fun(B) -> B /= Highest end, Blocks),
            {reply, {ok, Fetched}, State};
        false ->
            %% the syncer is higher than the syncee or there is a
            %% disjoint chain, so return [] to noop
            {reply, {ok, []}, State}
    end;
handle_call(get_blocks, _From, State) ->
    {reply, {ok, State#state.blocks}, State};
handle_call(get_status, _From, State) ->
    {reply, {ok, hbbft:status(State#state.hbbft)}, State};
handle_call(get_buf, _From, State) ->
    {reply, {ok, hbbft:buf(State#state.hbbft)}, State};
handle_call(Msg, _From, State) ->
    ?LOG_INFO("unhandled msg ~p~n", [Msg]),
    {reply, ok, State}.

handle_cast({hbbft, PeerID, Msg}, State = #state{hbbft=HBBFT, sk=SK}) ->
    NewState = handle_defers(dispatch(hbbft:handle_msg(maybe_deserialize_hbbft(HBBFT, SK), PeerID, Msg), {PeerID, Msg}, State)),
    {noreply, NewState};
handle_cast({block, NewBlock}, State=#state{sk=SK, hbbft=HBBFT}) ->
    case lists:member(NewBlock, State#state.blocks) of
        false ->
            ?LOG_INFO(#{description => "XXXXXXXX~n"}),
            %% a new block, check if it fits on our chain
            case verify_block_fit([NewBlock|State#state.blocks], tpke_privkey:public_key(SK)) of
                true ->
                    %% advance to the next round
                    ?LOG_INFO("~p skipping to next round~n", [self()]),
                    %% remove any transactions we have from our queue (drop the signature messages, they're not needed)
                    {NewHBBFT, _} = hbbft:finalize_round(maybe_deserialize_hbbft(HBBFT, SK), NewBlock#block.transactions, term_to_binary(NewBlock)),
                    NewState = dispatch(hbbft:next_round(maybe_deserialize_hbbft(NewHBBFT, SK)), undefined, State#state{blocks=[NewBlock | State#state.blocks]}),
                    {noreply, NewState#state{tempblock=undefined}};
                false ->
                    ?LOG_INFO(#{description => "invalid block proposed~n"}),
                    {noreply, State}
            end;
        true ->
            {noreply, State}
    end;
handle_cast(Msg, State) ->
    ?LOG_INFO("unhandled msg ~p~n", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    ?LOG_INFO("unhandled msg ~p~n", [Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
terminate(_Reason, _State) ->
    ?LOG_INFO("Terminating hbbft worker.", []),
    ok.

dispatch({NewHBBFT, {send, ToSend}}, _Msg, State) ->
    do_send(ToSend, State),
    State#state{hbbft=maybe_serialize_HBBFT(NewHBBFT, State#state.to_serialize)};
dispatch({NewHBBFT, {result, {transactions, _, Txns}}}, _Msg, State) ->
    NewBlock = case State#state.blocks of
                   [] ->
                       %% genesis block
                       #block{prev_hash= <<>>, transactions=Txns, signature= <<>>};
                   [PrevBlock|_Blocks] ->
                       #block{prev_hash=hash_block(PrevBlock), transactions=Txns, signature= <<>>}
               end,
    %% tell the badger to finish the round
    dispatch(hbbft:finalize_round(maybe_deserialize_hbbft(NewHBBFT, State#state.sk), Txns, term_to_binary(NewBlock)), undefined, State#state{tempblock=NewBlock});
dispatch({NewHBBFT, {result, {signature, Sig}}}, _Msg, State = #state{tempblock=NewBlock0}) ->
    NewBlock = NewBlock0#block{signature=Sig},
    case os:getenv("PARTISAN") of
        "true" ->
            lists:foreach(fun(Dest) ->
                try
                    Process = global:whereis_name(name(Dest)),
                    Node = node(Process),
                    Message = {block, NewBlock},
                    ok = partisan:cast_message(
                        Node,
                        Process,
                        Message,
                        #{channel => ?DEFAULT_CHANNEL}
                    )
                catch
                    _:_ ->
                        %% Node might have gone offline.
                        ok
                end
            end, lists:seq(0, State#state.n - 1));
        false ->
            [ gen_server:cast({global, name(Dest)}, {block, NewBlock}) || Dest <- lists:seq(0, State#state.n - 1)]
    end,
    dispatch(hbbft:next_round(maybe_deserialize_hbbft(NewHBBFT, State#state.sk)), undefined, State#state{blocks=[NewBlock|State#state.blocks], tempblock=undefined});
dispatch({NewHBBFT, ok}, _Msg, State) ->
    State#state{hbbft=maybe_serialize_HBBFT(NewHBBFT, State#state.to_serialize)};
dispatch(ignore, _Msg, State) ->
    State;
dispatch({_NewHBBFT, defer}, Msg, State) ->
    State#state{defer_queue=[Msg|State#state.defer_queue]};
dispatch({_NewHBBFT, already_started}, _Msg, State) ->
    State;
dispatch({NewHBBFT, Other}, Msg, State) ->
    ?LOG_INFO("UNHANDLED ~p ~p~n", [Other, Msg]),
    State#state{hbbft=maybe_serialize_HBBFT(NewHBBFT, State#state.to_serialize)};
dispatch(Other, Msg, State) ->
    ?LOG_INFO("UNHANDLED2 ~p ~p~n", [Other, Msg]),
    State.

do_send([], _) ->
    ok;
do_send([{unicast, Dest, Msg}|T], State) ->
    case os:getenv("PARTISAN") of
        "true" ->
            try
                Process = global:whereis_name(name(Dest)),
                Node = node(Process),
                Message = {hbbft, State#state.id, Msg},
                ?LOG_INFO("Sending partisan message to node ~p process ~p: ~p", [Node, Process, Message]),
                ok = partisan:cast_message(
                    Node,
                    Process,
                    Message,
                    #{channel => ?DEFAULT_CHANNEL}
                )
            catch
                _:_ ->
                    %% Node might have gone offline.
                    ok
            end;
        _ ->
            gen_server:cast({global, name(Dest)}, {hbbft, State#state.id, Msg})
    end,

    do_send(T, State);
do_send([{multicast, Msg}|T], State) ->
    case os:getenv("PARTISAN") of
        "true" ->
            lists:foreach(fun(Dest) ->
                try
                    Process = global:whereis_name(name(Dest)),
                    Node = node(Process),
                    Message = {hbbft, State#state.id, Msg},
                    ?LOG_INFO("Sending partisan message to node ~p process ~p: ~p", [Node, Process, Message]),
                    ok = partisan:cast_message(
                        Node, Process, Message, #{channel => ?DEFAULT_CHANNEL}
                    )
                catch
                    _:_ ->
                        %% Node might have gone offline.
                        ok
                end
            end, lists:seq(0, State#state.n - 1));
        _ ->
            [gen_server:cast({global, name(Dest)}, {hbbft, State#state.id, Msg}) || Dest <- lists:seq(0, State#state.n - 1)]
    end,

    do_send(T, State).


%% helper functions
name(N) ->
    list_to_atom(lists:flatten(["hbbft_worker_", integer_to_list(N)])).

hash_block(Block) ->
    crypto:hash(sha256, term_to_binary(Block)).

maybe_deserialize_hbbft(HBBFT, SK) ->
    case hbbft:is_serialized(HBBFT) of
        true -> hbbft:deserialize(HBBFT, SK);
        false -> HBBFT
    end.

maybe_serialize_HBBFT(HBBFT, ToSerialize) ->
    case hbbft:is_serialized(HBBFT) orelse not ToSerialize of
        true -> HBBFT;
        false -> element(1, hbbft:serialize(HBBFT, false))
    end.

handle_defers(State = #state{defer_queue=[]}) ->
    State;
handle_defers(State) ->
    lists:foldl(fun({PeerID, Msg}, Acc) ->
                        #state{hbbft=HBBFT, sk=SK} = Acc,
                         dispatch(hbbft:handle_msg(maybe_deserialize_hbbft(HBBFT, SK), PeerID, Msg), {PeerID, Msg}, State)
                end, State#state{defer_queue=[]}, lists:reverse(State#state.defer_queue)).
