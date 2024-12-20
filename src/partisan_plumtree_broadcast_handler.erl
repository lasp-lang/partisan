%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
%% @doc This module defines a behaviour to customise the implementation
%% for the operations performed by the {@link partisan_plumtree_broadcast}
%% server.
%%
%% == Callbacks ==
%% The behaviour defines the following callbacks:
%% <ul>
%% <li>`broadcast_data/1' - must return a two-tuple of message id and payload
%% from a given broadcast. Where the broadcasted message is
%% application-specific.
%% </li>
%% <li>`broadcast_channel/1' (optional) - Must return the channel to be used
%% when broadcasting data associate with this handler.
%% See {@link partisan_plumtree_broadcast:broadcast/2}.
%% </li>
%% </ul>
%%
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_plumtree_broadcast_handler).

-include("partisan.hrl").


%% Return a two-tuple of message id and payload from a given broadcast
-callback broadcast_data(any()) -> {MessageId :: any(), Payload :: any()}.


%% Return the channel to be used when broadcasting data associate with this
%% handler
-callback broadcast_channel() -> partisan:channel().

%% Given the message id and payload, merge the message in the local state.
%% If the message has already been received return `false', otherwise return `true'
-callback merge(any(), any()) -> boolean().

%% Return true if the message (given the message id) has already been received.
%% `false' otherwise
-callback is_stale(any()) -> boolean().

%% Return the message associated with the given message id. In some cases a
%% message has already been sent with information that subsumes the message
%% associated with the given message id. In this case, `stale' is returned.
-callback graft(any()) -> stale | {ok, any()} | {error, any()}.

%% Trigger an exchange between the local handler and the handler on the given
%% node.
%% How the exchange is performed is not defined but it should be performed as a
%% background process and ensure that it delivers any messages missing on
%% either the local or remote node.
%% The exchange does not need to account for messages in-flight when it is
%% started or broadcast during its operation. These can be taken care of in
%% future exchanges.
-callback exchange(node()) -> ok | {ok, pid()} | {error, term()} | ignore.


-optional_callbacks([broadcast_channel/0]).


