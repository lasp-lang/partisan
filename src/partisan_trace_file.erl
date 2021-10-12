%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(partisan_trace_file).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([read/1, write/2]).

read(TraceFile) ->
    %% Open file.
    {ok, TraceRef} = dets:open_file(TraceFile),

    %% Get number of keys.
    [{num_keys, NumKeys}] = dets:lookup(TraceRef, num_keys),

    %% Look them up.
    TraceLines = lists:foldl(fun(N, Acc) ->
        [{N, Entry}] = dets:lookup(TraceRef, N),
        Acc ++ [Entry]
    end, [], lists:seq(1, NumKeys)),

    %% Print output.
    % lists:foreach(fun(Line) ->
    %     logger:info("~p~n", [Line])
    % end, TraceLines),

    %% Close table.
    dets:close(TraceRef),

    {ok, TraceLines}.

write(TraceFile, TraceLines) ->
    %% Number trace.
    {NumEntries, NumberedTrace0} = lists:foldl(fun(Line, {N, Lines}) -> 
        {N + 1, Lines ++ [{N, Line}]} end, 
    {1, []}, TraceLines),

    %% Add row containing number of keys.
    NumberedTrace = [{num_keys, NumEntries - 1}] ++ NumberedTrace0,

    %% Remove existing trace file.
    os:cmd("rm " ++ TraceFile),

    %% Write out contents of the new trace file.
    {ok, TraceTable} = dets:open_file(trace, [{file, TraceFile}]),
    dets:insert(TraceTable, NumberedTrace),
    ok = dets:close(TraceTable),

    ok.