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

-module(partisan_transform).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

%% Public API
-export([parse_transform/2]).

%% @private
parse_transform(AST, _Options) ->
    walk_ast([], AST).

%% @private
walk_ast(Acc, []) ->
    lists:reverse(Acc);
walk_ast(Acc, [{attribute, _, module, {_Module, _PmodArgs}}=H|T]) ->
    walk_ast([H|Acc], T);
walk_ast(Acc, [{attribute, _, module, _Module}=H|T]) ->
    walk_ast([H|Acc], T);
walk_ast(Acc, [{function, Line, Name, Arity, Clauses}|T]) ->
    walk_ast([{function, Line, Name, Arity,
                walk_clauses([], Clauses)}|Acc], T);
walk_ast(Acc, [{attribute, _, record, {_Name, _Fields}}=H|T]) ->
    walk_ast([H|Acc], T);
walk_ast(Acc, [H|T]) ->
    walk_ast([H|Acc], T).

%% @private
walk_clauses(Acc, []) ->
    lists:reverse(Acc);
walk_clauses(Acc, [{clause, Line, Arguments, Guards, Body}|T]) ->
    walk_clauses([{clause, 
                   Line, Arguments, Guards, walk_body([], Body)}|Acc], 
                 T).

%% @private
walk_body(Acc, []) ->
    lists:reverse(Acc);
walk_body(Acc, [H|T]) ->
    walk_body([transform_statement(H)|Acc], T).

%% @private
transform_statement({op, Line, '!', 
                     {var, Line, RemotePid}, {Type, Line, Message}}) ->
    {call, Line, {remote, Line, 
                  {atom, Line, partisan_peer_service_manager}, {atom, Line, forward_message}},
        [{var, Line, RemotePid}, {Type, Line, Message}]};
transform_statement({match, Line, {var, Line, RemotePid}, {call, Line, {atom, Line, self}, []}}) ->
    {match, Line, {var, Line, RemotePid}, {call, Line, {remote, Line, {atom, Line, partisan_util}, {atom, Line, pid}}, []}};
transform_statement(Stmt) ->
    Stmt.