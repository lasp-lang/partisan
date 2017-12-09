%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(partisan_plumtree_util).

-export([build_tree/3,
         log/2, 
         log/3]).

%% @doc Convert a list of elements into an N-ary tree. This conversion
%%      works by treating the list as an array-based tree where, for
%%      example in a binary 2-ary tree, a node at index i has children
%%      2i and 2i+1. The conversion also supports a "cycles" mode where
%%      the array is logically wrapped around to ensure leaf nodes also
%%      have children by giving them backedges to other elements.

-spec build_tree(N :: integer(), Nodes :: [term()], Opts :: [term()])
                -> orddict:orddict().
build_tree(N, Nodes, Opts) ->
    Expand =
        case lists:member(cycles, Opts) of
            true ->
                lists:flatten(lists:duplicate(N+1, Nodes));
            false ->
                Nodes
        end,
    {Tree, _} =
        lists:foldl(fun(Elm, {Result, Worklist}) ->
                            Len = erlang:min(N, length(Worklist)),
                            {Children, Rest} = lists:split(Len, Worklist),
                            NewResult = [{Elm, Children} | Result],
                            {NewResult, Rest}
                    end, {[], tl(Expand)}, Nodes),
    orddict:from_list(Tree).

-spec log(debug | info | error,
          String :: string(),
          Args :: list(term())) -> ok.
-ifdef(TEST).

log(Level, String) ->
    log(Level, String, []).

log(debug, String, Args) ->
    lager:debug(String, Args);
log(info, String, Args) ->
    lager:info(String, Args);
log(error, String, Args) ->
    lager:error(String, Args).

-else.

log(_Level, _String) -> ok.
log(_Level, _String, _Args) -> ok.

-endif.

%%
%% Tests
%%
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

arity_test() ->
    %% 1-ary tree
    ?assertEqual([{node1, []}], orddict:to_list(build_tree(1, [node1], []))),
    ?assertEqual([{node1, [node2]},
                  {node2, []}], orddict:to_list(build_tree(1, [node1, node2], []))),
    ?assertEqual([{node1, [node2]},
                  {node2, [node3]},
                  {node3, []}], orddict:to_list(build_tree(1, [node1, node2, node3], []))),
    ?assertEqual([{node1, [node2]},
                  {node2, [node3]},
                  {node3, [node4]},
                  {node4, []}], orddict:to_list(build_tree(1, [node1, node2,
                                                               node3, node4], []))),

    %% 2-ary tree
    ?assertEqual([{node1, []}], orddict:to_list(build_tree(2, [node1], []))),
    ?assertEqual([{node1, [node2]},
                  {node2, []}], orddict:to_list(build_tree(2, [node1, node2], []))),
    ?assertEqual([{node1, [node2, node3]},
                  {node2, []},
                  {node3, []}], orddict:to_list(build_tree(2, [node1, node2, node3], []))),
    ?assertEqual([{node1, [node2, node3]},
                  {node2, [node4]},
                  {node3, []},
                  {node4, []}], orddict:to_list(build_tree(2, [node1, node2,
                                                               node3, node4], []))),
    ?assertEqual([{node1, [node2, node3]},
                  {node2, [node4, node5]},
                  {node3, []},
                  {node4, []},
                  {node5, []}], orddict:to_list(build_tree(2, [node1, node2,
                                                               node3, node4,
                                                               node5], []))),
    ?assertEqual([{node1, [node2, node3]},
                  {node2, [node4, node5]},
                  {node3, [node6]},
                  {node4, []},
                  {node5, []},
                  {node6, []}], orddict:to_list(build_tree(2, [node1, node2,
                                                               node3, node4,
                                                               node5, node6], []))),

    %% 3-ary tree
    ?assertEqual([{node1, []}], orddict:to_list(build_tree(3, [node1], []))),
    ?assertEqual([{node1, [node2]},
                  {node2, []}], orddict:to_list(build_tree(3, [node1, node2], []))),
    ?assertEqual([{node1, [node2, node3]},
                  {node2, []},
                  {node3, []}], orddict:to_list(build_tree(3, [node1, node2, node3], []))),
    ?assertEqual([{node1, [node2, node3, node4]},
                  {node2, []},
                  {node3, []},
                  {node4, []}], orddict:to_list(build_tree(3, [node1, node2,
                                                               node3, node4], []))),
    ?assertEqual([{node1, [node2, node3, node4]},
                  {node2, [node5]},
                  {node3, []},
                  {node4, []},
                  {node5, []}], orddict:to_list(build_tree(3, [node1, node2,
                                                               node3, node4,
                                                               node5], []))),
    ?assertEqual([{node1, [node2, node3, node4]},
                  {node2, [node5, node6]},
                  {node3, []},
                  {node4, []},
                  {node5, []},
                  {node6, []}], orddict:to_list(build_tree(3, [node1, node2,
                                                               node3, node4,
                                                               node5, node6], []))),
    ?assertEqual([{node1, [node2, node3, node4]},
                  {node2, [node5, node6, node7]},
                  {node3, []},
                  {node4, []},
                  {node5, []},
                  {node6, []},
                  {node7, []}], orddict:to_list(build_tree(3, [node1, node2,
                                                               node3, node4,
                                                               node5, node6,
                                                               node7], []))),
    ?assertEqual([{node1, [node2, node3, node4]},
                  {node2, [node5, node6, node7]},
                  {node3, [node8]},
                  {node4, []},
                  {node5, []},
                  {node6, []},
                  {node7, []},
                  {node8, []}], orddict:to_list(build_tree(3, [node1, node2,
                                                               node3, node4,
                                                               node5, node6,
                                                               node7, node8], []))).

cycles_test() ->
    %% 1-ary tree
    ?assertEqual([{node1, [node1]}], orddict:to_list(build_tree(1, [node1], [cycles]))),
    ?assertEqual([{node1, [node2]},
                  {node2, [node1]}], orddict:to_list(build_tree(1, [node1, node2], [cycles]))),
    ?assertEqual([{node1, [node2]},
                  {node2, [node3]},
                  {node3, [node1]}], orddict:to_list(build_tree(1, [node1, node2, node3], [cycles]))),
    ?assertEqual([{node1, [node2]},
                  {node2, [node3]},
                  {node3, [node4]},
                  {node4, [node1]}], orddict:to_list(build_tree(1, [node1, node2,
                                                                   node3, node4], [cycles]))),

    %% 2-ary tree
    ?assertEqual([{node1, [node1, node1]}], orddict:to_list(build_tree(2, [node1], [cycles]))),
    ?assertEqual([{node1, [node2, node1]},
                  {node2, [node2, node1]}], orddict:to_list(build_tree(2, [node1, node2], [cycles]))),
    ?assertEqual([{node1, [node2, node3]},
                  {node2, [node1, node2]},
                  {node3, [node3, node1]}], orddict:to_list(build_tree(2, [node1, node2, node3], [cycles]))),
    ?assertEqual([{node1, [node2, node3]},
                  {node2, [node4, node1]},
                  {node3, [node2, node3]},
                  {node4, [node4, node1]}], orddict:to_list(build_tree(2, [node1, node2,
                                                                           node3, node4], [cycles]))),
    ?assertEqual([{node1, [node2, node3]},
                  {node2, [node4, node5]},
                  {node3, [node1, node2]},
                  {node4, [node3, node4]},
                  {node5, [node5, node1]}], orddict:to_list(build_tree(2, [node1, node2,
                                                                           node3, node4,
                                                                           node5], [cycles]))),
    ?assertEqual([{node1, [node2, node3]},
                  {node2, [node4, node5]},
                  {node3, [node6, node1]},
                  {node4, [node2, node3]},
                  {node5, [node4, node5]},
                  {node6, [node6, node1]}], orddict:to_list(build_tree(2, [node1, node2,
                                                               node3, node4,
                                                               node5, node6], [cycles]))),

    %% 3-ary tree
    ?assertEqual([{node1, [node1, node1, node1]}], orddict:to_list(build_tree(3, [node1], [cycles]))),
    ?assertEqual([{node1, [node2, node1, node2]},
                  {node2, [node1, node2, node1]}], orddict:to_list(build_tree(3, [node1, node2], [cycles]))),
    ?assertEqual([{node1, [node2, node3, node1]},
                  {node2, [node2, node3, node1]},
                  {node3, [node2, node3, node1]}], orddict:to_list(build_tree(3, [node1, node2, node3], [cycles]))),
    ?assertEqual([{node1, [node2, node3, node4]},
                  {node2, [node1, node2, node3]},
                  {node3, [node4, node1, node2]},
                  {node4, [node3, node4, node1]}], orddict:to_list(build_tree(3, [node1, node2,
                                                                                  node3, node4], [cycles]))),
    ?assertEqual([{node1, [node2, node3, node4]},
                  {node2, [node5, node1, node2]},
                  {node3, [node3, node4, node5]},
                  {node4, [node1, node2, node3]},
                  {node5, [node4, node5, node1]}], orddict:to_list(build_tree(3, [node1, node2,
                                                                                  node3, node4,
                                                                                  node5], [cycles]))),
    ?assertEqual([{node1, [node2, node3, node4]},
                  {node2, [node5, node6, node1]},
                  {node3, [node2, node3, node4]},
                  {node4, [node5, node6, node1]},
                  {node5, [node2, node3, node4]},
                  {node6, [node5, node6, node1]}], orddict:to_list(build_tree(3, [node1, node2,
                                                                                  node3, node4,
                                                                                  node5, node6], [cycles]))).
-endif.
