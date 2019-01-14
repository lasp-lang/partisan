%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(gossip_demers).

-include("partisan.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

%% API
-export([start_link/1,
         gossip/2,
         broadcast/2,
         update/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {membership}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Nodes) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Nodes], []).

%% @doc Gossip.
%% 
%% Given the membership known at this node, select `fanout' nodes
%% uniformly from the membership to transmit the message to.
%%
broadcast(ServerRef, Message) ->
    gossip(ServerRef, Message).

%% @doc Gossip.
%% 
%% Given the membership known at this node, select `fanout' nodes
%% uniformly from the membership to transmit the message to.
%%
gossip(ServerRef, Message) ->
    gen_server:cast(?MODULE, {gossip, ServerRef, Message}).

%% @doc Notifies us of membership update.
update(LocalState0) ->
    LocalState = partisan_peer_service:decode(LocalState0),
    gen_server:cast(?MODULE, {update, LocalState}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Nodes]) ->
    %% Seed the random number generator.
    partisan_config:seed(),

    %% Register membership update callback.
    %% partisan_peer_service:add_sup_callback(fun ?MODULE:update/1),

    %% Open ETS table to track received messages.
    ?MODULE = ets:new(?MODULE, [set, named_table, public]),

    %% Start with empty membership.
    Membership = Nodes,

    {ok, #state{membership=Membership}}.

%% @private
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

%% @private
handle_cast({gossip, ServerRef, Message}, #state{membership=Membership}=State) ->
    Manager = manager(),

    %% Generate message id.
    MyNode = partisan_peer_service_manager:mynode(),
    Id = {MyNode, erlang:unique_integer([monotonic, positive])},

    %% Forward to process.
    partisan_util:process_forward(ServerRef, Message),

    %% Store outgoing message.
    true = ets:insert(?MODULE, {Id, Message}),

    MyNode = partisan_peer_service_manager:mynode(),
    GossipMembers = select_random_sublist(Membership, ?GOSSIP_FANOUT),

    lists:foreach(fun(N) ->
        lager:info("~p: sending gossip message to node ~p: ~p", [node(), N, Message]),
        Manager:forward_message(N, ?GOSSIP_CHANNEL, ?MODULE, {gossip, Id, ServerRef, Message})
    end, GossipMembers -- [MyNode]),

    {noreply, State};
handle_cast({update, Membership0}, State) ->
    Membership = lists:usort(Membership0), %% Must sort list or random selection with seed is *nondeterministic.*
    {noreply, State#state{membership=Membership}};
handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
%% Incoming messages.
handle_info({gossip, Id, ServerRef, Message}, #state{membership=Membership}=State) ->
    lager:info("~p received value from gossip: ~p", [node(), Message]),
    Manager = manager(),

    case ets:lookup(?MODULE, Id) of
        [] ->
            %% Forward to process.
            partisan_util:process_forward(ServerRef, Message),

            %% Store.
            true = ets:insert(?MODULE, {Id, Message}),

            %% Forward message.
            MyNode = partisan_peer_service_manager:mynode(),
            GossipMembers = select_random_sublist(Membership, ?GOSSIP_FANOUT),
            lager:info("~p: forwarding gossip message, selecting from members: ~p, gossip members: ~p", [node(), Membership, GossipMembers]),

            lists:foreach(fun(N) ->
                lager:info("~p: forwarding gossip message to node ~p: ~p", [node(), N, Message]),
                Manager:forward_message(N, ?GOSSIP_CHANNEL, ?MODULE, {gossip, Id, ServerRef, Message})
            end, GossipMembers -- [MyNode]),

            lager:info("Forwarding to gossip members: ~p", [GossipMembers]),

            %% Drop oldest message.
            Info = ets:info(?MODULE),
            case proplists:get_value(size, Info, undefined) of
                undefined ->
                    ok;
                Size ->
                    case Size > ?GOSSIP_GC_MIN_SIZE of
                        true ->
                            case ets:first(?MODULE) of
                                '$end_of_table' ->
                                    ok;
                                Key ->
                                    ets:delete(?MODULE, Key)
                            end;
                        false ->
                            ok
                    end
            end,

            ok;
        _ ->
            ok
    end,

    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
select_random_sublist(Membership, K) ->
    lists:sublist(shuffle(Membership), K).

%% @reference http://stackoverflow.com/questions/8817171/shuffling-elements-in-a-list-randomly-re-arrange-list-elements/8820501#8820501
shuffle(L) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), N} || N <- L])].

%% @private
manager() ->
    partisan_config:get(partisan_peer_service_manager).