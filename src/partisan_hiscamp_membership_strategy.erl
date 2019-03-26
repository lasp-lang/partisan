-module(partisan_hiscamp_membership_strategy).
-behavior(gen_server).

-export([start_link/0]).
-export([init/1, handle_cast/2, handle_call/3]).

%% Client API
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%% server functions
init([]) -> {ok, []}.

handle_call({forward, MembershipStrategyState0, ProtocolMessage}, _From, State) ->
    ({ok, Membership, OutgoingMessages, MembershipStrategyState} = 
     partisan_scamp_v2_membership_strategy:handle_message(MembershipStrategyState0,
                                                          ProtocolMessage)),
    Reply = {ok, Membership, OutgoingMessages, MembershipStrategyState},
    NewState = State,
    {reply, Reply, NewState}.

%% handle_cast doesn't do anything for now
%% don't know what the request might be so we ignore it
handle_cast(_Request, State) ->
    {noreply, State}.

