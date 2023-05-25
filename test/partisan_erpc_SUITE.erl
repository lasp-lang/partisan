%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2020-2022. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% %CopyrightEnd%
%%
-module(partisan_erpc_SUITE).

-export([all/0, suite/0,groups/0,init_per_suite/1, end_per_suite/1, 
     init_per_group/2,end_per_group/2]).
-export([call/1, call_reqtmo/1, call_against_old_node/1, cast/1,
         send_request/1, send_request_fun/1,
         send_request_receive_reqtmo/1,
         send_request_wait_reqtmo/1,
         send_request_check_reqtmo/1,
         send_request_against_old_node/1,
         multicall/1, multicall_reqtmo/1,
         multicall_recv_opt/1,
         multicall_recv_opt2/1,
         multicall_recv_opt3/1,
         multicast/1,
         timeout_limit/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([call_func1/1, call_func2/1, call_func4/4]).

-export([f/0, f2/0]).

-include_lib("common_test/include/ct.hrl").

suite() ->
    [{ct_hooks,[ts_install_cth]},
     {timetrap,{minutes,2}}].

all() -> 
    [call,
     call_reqtmo,
     call_against_old_node,
     cast,
     send_request,
     send_request_fun,
     send_request_receive_reqtmo,
     send_request_wait_reqtmo,
     send_request_check_reqtmo,
     send_request_against_old_node,
     multicall,
     multicall_reqtmo,
     multicall_recv_opt,
     multicall_recv_opt2,
     multicall_recv_opt3,
     multicast,
     timeout_limit].

groups() -> 
    [].

init_per_testcase(Func, Config) when is_atom(Func), is_list(Config) ->
    partisan_support:start_disterl(),
    erlang:is_alive() orelse ct:fail("Runner not in distribution mode"),
    application:ensure_all_started(partisan),
    [{testcase, Func}|Config].

end_per_testcase(_Func, _Config) ->
    partisan_support_otp:stop_all_nodes(), % stop all peers
    application:stop(partisan), % stop partisan at runner
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, Config) ->
    Config.

call(Config) when is_list(Config) ->
    call_test(Config).

call_test(Config) ->
    call_test(partisan:node(), 10000),
    call_test(partisan:node(), infinity),
    try
        partisan_erpc:call(partisan:node(), timer, sleep, [100], 10),
        ct:fail(unexpected)
    catch
        error:{erpc, timeout} ->
            ok
    end,
    try
        partisan_erpc:call(partisan:node(), fun () -> timer:sleep(100) end, 10),
        ct:fail(unexpected)
    catch
        error:{erpc, timeout} ->
            ok
    end,
    {ok, Node} = start_node(Config),
    call_test(Node, 10000),
    call_test(Node, infinity),
    try
        partisan_erpc:call(Node, timer, sleep, [100], 10),
        ct:fail(unexpected)
    catch
        error:{erpc, timeout} ->
            ok
    end,
    try
        partisan_erpc:call(Node, fun () -> timer:sleep(100) end, 10),
        ct:fail(unexpected)
    catch
        error:{erpc, timeout} ->
            ok
    end,
    try
        partisan_erpc:call(Node, erlang, halt, []),
        ct:fail(unexpected)
    catch
        error:{erpc, noconnection} ->
            ok
    end,
    try
        partisan_erpc:call(Node, fun () -> partisan:node() end),
        ct:fail(unexpected)
    catch
        error:{erpc, noconnection} ->
            ok
    end,
    try
        partisan_erpc:call(Node, partisan, node, []),
        ct:fail(unexpected)
    catch
        error:{erpc, noconnection} ->
            ok
    end,
    try
        partisan_erpc:call(badnodename, partisan, node, []),
        ct:fail(unexpected)
    catch
        error:{erpc, noconnection} ->
            ok
    end,

    receive after 1000 -> ok end,
    [] = flush([]),
    ok.

call_test(Node, Timeout) ->
    io:format("call_test(~p, ~p)~n", [Node, Timeout]),
    Node = partisan_erpc:call(Node, partisan, node, [], Timeout),
    try
        partisan_erpc:call(Node, erlang, error, [oops|invalid], Timeout),
        ct:fail(unexpected)
    catch
        error:{exception, badarg, [{erlang,apply,[erlang,error,[oops|invalid]],_}]} ->
            ok
    end,
    try
        partisan_erpc:call(Node, erlang, error, [oops], Timeout),
        ct:fail(unexpected)
    catch
        error:{exception, oops, [{erlang,error,[oops],_}]} ->
            ok
    end,
    try
        partisan_erpc:call(Node, erlang, exit, [oops], Timeout),
        ct:fail(unexpected)
    catch
        exit:{exception, oops} ->
            ok
    end,
    try
        partisan_erpc:call(Node, fun () -> erlang:exit(oops) end, Timeout),
        ct:fail(unexpected)
    catch
        exit:{exception, oops} ->
            ok
    end,
    try
        partisan_erpc:call(Node, erlang, throw, [oops], Timeout),
        ct:fail(unexpected)
    catch
        throw:oops ->
            ok
    end,
    try
        partisan_erpc:call(Node, fun () -> erlang:throw(oops) end, Timeout),
        ct:fail(unexpected)
    catch
        throw:oops ->
            ok
    end,
    case {node() == Node, Timeout == infinity} of
        {true, true} ->
            %% This would kill the test since local calls
            %% without timeout is optimized to execute in
            %% calling process itself...
            ok;
        _ ->
            ExitSignal = fun () ->
                                 partisan:exit(partisan:self(), oops),
                                 receive after infinity -> ok end
                         end,
            try
                partisan_erpc:call(Node, ExitSignal, Timeout),
                ct:fail(unexpected)
            catch
                exit:{signal, oops} ->
                    ok
            end,
            try
                partisan_erpc:call(Node, erlang, apply, [ExitSignal, []], Timeout),
                ct:fail(unexpected)
            catch
                exit:{signal, oops} ->
                    ok
            end
    end,
    try
        partisan_erpc:call(Node, ?MODULE, call_func1, [boom], Timeout),
        ct:fail(unexpected)
    catch
        error:{exception,
               {exception,
                boom,
                [{?MODULE, call_func3, A2, _},
                 {?MODULE, call_func2, 1, _}]},
               [{erpc, call, A1, _},
                {?MODULE, call_func1, 1, _}]}
          when ((A1 == 5)
                orelse (A1 == [Node, ?MODULE, call_func2, [boom]]))
               andalso ((A2 == 1)
                        orelse (A2 == [boom])) ->
            ok
    end,
    try
        partisan_erpc:call(Node, fun () -> ?MODULE:call_func1(boom) end, Timeout),
        ct:fail(unexpected)
    catch
        error:{exception,
               {exception,
                boom,
                [{?MODULE, call_func3, A4, _},
                 {?MODULE, call_func2, 1, _}]},
               [{erpc, call, A3, _},
                {?MODULE, call_func1, 1, _},
                {erlang, apply, 2, _}]}
          when ((A3 == 5)
                orelse (A3 == [Node, ?MODULE, call_func2, [boom]]))
               andalso ((A4 == 1)
                        orelse (A4 == [boom])) ->
            ok
    end,
    try
        call_func4(Node, partisan:node(), 10, Timeout),
        ct:fail(unexpected)
    catch
        error:Error1 ->
%%%            io:format("Error1=~p~n", [Error1]),
            check_call_func4_error(Error1, 10)
    end,
    try
        call_func4(partisan:node(), Node, 5, Timeout),
        ct:fail(unexpected)
    catch
        error:Error2 ->
%%%            io:format("Error2=~p~n", [Error2]),
            check_call_func4_error(Error2, 5)
    end,
    ok.

check_call_func4_error({exception,
                         badarg,
                         [{?MODULE, call_func5, _, _},
                          {?MODULE, call_func4, _, _}]},
                         0) ->
    ok;
check_call_func4_error({exception,
                        Exception,
                        [{erpc, call, _, _},
                         {?MODULE, call_func5, _, _},
                         {?MODULE, call_func4, _, _}]},
                       N) ->
    check_call_func4_error(Exception, N-1).
    
call_func1(X) ->
    partisan_erpc:call(partisan:node(), ?MODULE, call_func2, [X]),
    ok.

call_func2(X) ->
    call_func3(X),
    ok.

call_func3(X) ->
    erlang:error(X, [X]).
    
call_func4(A, B, N, T) ->
    call_func5(A, B, N, T),
    ok.

call_func5(A, B, N, T) when N >= 0 ->
    partisan_erpc:call(A, ?MODULE, call_func4, [B, A, N-1, T], T),
    ok;
call_func5(_A, _B, _N, _T) ->
    erlang:error(badarg).

call_against_old_node(Config) ->
    case start_22_node(Config) of
        {ok, Node22} ->
            try
                partisan_erpc:call(Node22, partisan, node, []),
                ct:fail(unexpected)
            catch
                error:{erpc, notsup} ->
                    ok
            end,
            stop_node(Node22),
            ok;
        _ ->
        {skipped, "No OTP 22 available"}
    end.

call_reqtmo(Config) when is_list(Config) ->
    Fun = fun (Node, SendMe, Timeout) ->
                  try
                      partisan_erpc:call(Node, erlang, send,
                                [partisan:self(), SendMe], Timeout),
                      ct:fail(unexpected)
                  catch
                      error:{erpc, timeout} -> ok
                  end
          end,
    reqtmo_test(Config, Fun).

reqtmo_test(Config, Test) ->
    %% Tests that we time out in time also when the request itself
    %% does not get through. A typical issue we have had
    %% in the past, is that the timeout has not triggered until
    %% the request has gotten through...
    
    Timeout = 500,
    WaitBlock = 100,
    BlockTime = 1000,

    {ok, Node} = start_node(Config),

    partisan_erpc:call(Node, erts_debug, set_internal_state, [available_internal_state,
                                                     true]),
    
    SendMe = partisan:make_ref(),

    partisan_erpc:cast(Node, erts_debug, set_internal_state, [block, BlockTime]),
    receive after WaitBlock -> ok end,
    
    Start = erlang:monotonic_time(),

    Test(Node, SendMe, Timeout),

    Stop = erlang:monotonic_time(),
    Time = erlang:convert_time_unit(Stop-Start, native, millisecond),
    io:format("Actual time: ~p ms~n", [Time]),
    true = Time >= Timeout,
    true = Time =< Timeout + 200,
    
    receive SendMe -> ok end,
    
    receive UnexpectedMsg -> ct:fail({unexpected_message, UnexpectedMsg})
    after 0 -> ok
    end,
    
    stop_node(Node),
    
    {comment,
     "Timeout = " ++ integer_to_list(Timeout)
     ++ " Actual = " ++ integer_to_list(Time)}.

cast(Config) when is_list(Config) ->
    %% silently fail
    ok = partisan_erpc:cast(badnodename, erlang, send, [hej]),

    try
        partisan_erpc:cast(<<"node">>, erlang, send, [hej]),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} -> ok
    end,
    try
        partisan_erpc:cast(partisan:node(), erlang, send, hej),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} -> ok
    end,
    try
        partisan_erpc:cast(partisan:node(), "erlang", send, [hej]),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} -> ok
    end,
    try
        partisan_erpc:cast(partisan:node(), erlang, partisan:make_ref(), [hej]),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} -> ok
    end,

    partisan_erpc:cast(partisan:node(), erlang, send, [partisan:self()|blupp]), %% silent remote error...

    Me = partisan:self(),
    Ok1 = partisan:make_ref(),
    ok = partisan_erpc:cast(partisan:node(), erlang, send, [Me, {mfa, Ok1}]),
    receive
        {mfa, Ok1} -> ok
    end,
    ok = partisan_erpc:cast(partisan:node(), fun () -> Me ! {a_fun, Ok1} end),
    receive
        {a_fun, Ok1} -> ok
    end,
    {ok, Node} = start_node(Config),
    Ok2 = partisan:make_ref(),
    ok = partisan_erpc:cast(Node, erlang, send, [Me, {mfa, Ok2}]),
    receive
        {mfa, Ok2} -> ok
    end,
    ok = partisan_erpc:cast(Node, fun () -> Me ! {a_fun, Ok2} end),
    receive
        {a_fun, Ok2} -> ok
    end,

    ok = partisan_erpc:cast(Node, erlang, halt, []),

    monitor_node(Node, true),
    receive {nodedown, Node} -> ok end,

    ok = partisan_erpc:cast(Node, erlang, send, [Me, wont_reach_me]),

    receive after 1000 -> ok end,
    [] = flush([]),

    case start_22_node(Config) of
        {ok, Node22} ->
            ok = partisan_erpc:cast(Node, erlang, send, [Me, wont_reach_me]),
            ok = partisan_erpc:cast(Node, fun () -> Me ! wont_reach_me end),
            receive
                Msg -> ct:fail({unexpected_message, Msg})
            after
                2000 -> ok
            end,
            stop_node(Node22),
            {comment, "Tested against OTP 22 as well"};
        _ ->
            {comment, "No tested against OTP 22"}
    end.

send_request(Config) when is_list(Config) ->
    %% Note: First part of nodename sets response delay in seconds.
    PA = filename:dirname(code:which(?MODULE)),
    NodeArgs = [{args,"-pa "++ PA}],
    {ok,Node1} = test_server:start_node('1_erpc_SUITE_call', slave, NodeArgs),
    {ok,Node2} = test_server:start_node('10_erpc_SUITE_call', slave, NodeArgs),
    {ok,Node3} = test_server:start_node('20_erpc_SUITE_call', slave, NodeArgs),
    ReqId1 = partisan_erpc:send_request(Node1, ?MODULE, f, []),
    ReqId2 = partisan_erpc:send_request(Node2, ?MODULE, f, []),
    ReqId3 = partisan_erpc:send_request(Node3, ?MODULE, f, []),
    ReqId4 = partisan_erpc:send_request(Node1, erlang, error, [bang]),
    ReqId5 = partisan_erpc:send_request(Node1, ?MODULE, f, []),

    try
        partisan_erpc:receive_response(ReqId4, 1000)
    catch
        error:{exception, bang, [{erlang,error,[bang],_}]} ->
            ok
    end,
    try
        partisan_erpc:receive_response(ReqId5, 10)
    catch
        error:{erpc, timeout} ->
            ok
    end,

    %% Test fast timeouts.
    no_response = partisan_erpc:wait_response(ReqId2),
    no_response = partisan_erpc:wait_response(ReqId2, 10),

    %% Let Node1 finish its work before yielding.
    ct:sleep({seconds,2}),
    {hej,_,Node1} = partisan_erpc:receive_response(ReqId1),

    %% Wait for the Node2 and Node3.
    {response,{hej,_,Node2}} = partisan_erpc:wait_response(ReqId2, infinity),
    {hej,_,Node3} = partisan_erpc:receive_response(ReqId3),

    try
        partisan_erpc:receive_response(ReqId5, 10),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} ->
            ok
    end,

    receive
        Msg0 -> ct:fail(Msg0)
    after 0 -> ok
    end,

    stop_node(Node1),
    stop_node(Node2),
    stop_node(Node3),

    [] = flush([]),


    {ok, Node4} = start_node(Config),

    ReqId6 = partisan_erpc:send_request(Node4, partisan, node, []),
    
    no_response = partisan_erpc:check_response({response, Node1}, ReqId6),
    no_response = partisan_erpc:check_response(ReqId6, ReqId6),
    receive
        Msg1 ->
            {response, Node4} = partisan_erpc:check_response(Msg1, ReqId6)
    end,

    ReqId7 = partisan_erpc:send_request(Node4, erlang, halt, []),
    try
        partisan_erpc:receive_response(ReqId7),
        ct:fail(unexpected)
    catch
        error:{erpc, noconnection} -> ok
    end,

    ReqId8 = partisan_erpc:send_request(Node4, partisan, node, []),
    receive
        Msg2 ->
            try
                partisan_erpc:check_response(Msg2, ReqId8),
                ct:fail(unexpected)
            catch
                error:{erpc, noconnection} ->
                    ok
            end
    end,

    [] = flush([]),

    case start_22_node(Config) of
        {ok, Node5} ->
            ReqId9 = partisan_erpc:send_request(Node5, partisan, node, []),
            try
                partisan_erpc:receive_response(ReqId9),
                ct:fail(unexpected)
            catch
                error:{erpc, notsup} -> ok
            end,

            stop_node(Node5),

            [] = flush([]),

            ok;
        _ ->
            {comment, "No test against OTP 22 node performed"}
    end.

send_request_fun(Config) when is_list(Config) ->
    %% Note: First part of nodename sets response delay in seconds.
    PA = filename:dirname(code:which(?MODULE)),
    NodeArgs = [{args,"-pa "++ PA}],
    {ok,Node1} = test_server:start_node('1_erpc_SUITE_call', slave, NodeArgs),
    {ok,Node2} = test_server:start_node('10_erpc_SUITE_call', slave, NodeArgs),
    {ok,Node3} = test_server:start_node('20_erpc_SUITE_call', slave, NodeArgs),
    ReqId1 = partisan_erpc:send_request(Node1, fun () -> ?MODULE:f() end),
    ReqId2 = partisan_erpc:send_request(Node2, fun () -> ?MODULE:f() end),
    ReqId3 = partisan_erpc:send_request(Node3, fun () -> ?MODULE:f() end),
    ReqId4 = partisan_erpc:send_request(Node1, fun () -> erlang:error(bang) end),
    ReqId5 = partisan_erpc:send_request(Node1, fun () -> ?MODULE:f() end),

    try
        partisan_erpc:receive_response(ReqId4, 1000)
    catch
        error:{exception, bang, [{?MODULE, _, _, _},
                                 {erlang,apply,2,_}]} ->
            ok
    end,
    try
        partisan_erpc:receive_response(ReqId5, 10)
    catch
        error:{erpc, timeout} ->
            ok
    end,

    %% Test fast timeouts.
    no_response = partisan_erpc:wait_response(ReqId2),
    no_response = partisan_erpc:wait_response(ReqId2, 10),

    %% Let Node1 finish its work before yielding.
    ct:sleep({seconds,2}),
    {hej,_,Node1} = partisan_erpc:receive_response(ReqId1),

    %% Wait for the Node2 and Node3.
    {response,{hej,_,Node2}} = partisan_erpc:wait_response(ReqId2, infinity),
    {hej,_,Node3} = partisan_erpc:receive_response(ReqId3),

    try
        partisan_erpc:receive_response(ReqId5, 10),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} ->
            ok
    end,

    receive
        Msg0 -> ct:fail(Msg0)
    after 0 -> ok
    end,

    stop_node(Node1),
    stop_node(Node2),
    stop_node(Node3),

    {ok, Node4} = start_node(Config),

    ReqId6 = partisan_erpc:send_request(Node4, fun () -> partisan:node() end),
    
    no_response = partisan_erpc:check_response({response, Node1}, ReqId6),
    no_response = partisan_erpc:check_response(ReqId6, ReqId6),
    receive
        Msg1 ->
            {response, Node4} = partisan_erpc:check_response(Msg1, ReqId6)
    end,

    ReqId7 = partisan_erpc:send_request(Node4, erlang, halt, []),
    try
        partisan_erpc:receive_response(ReqId7),
        ct:fail(unexpected)
    catch
        error:{erpc, noconnection} -> ok
    end,

    ReqId8 = partisan_erpc:send_request(Node4, partisan, node, []),
    receive
        Msg2 ->
            try
                partisan_erpc:check_response(Msg2, ReqId8),
                ct:fail(unexpected)
            catch
                error:{erpc, noconnection} ->
                    ok
            end
    end,

    [] = flush([]),

    case start_22_node(Config) of
        {ok, Node5} ->
            ReqId9 = partisan_erpc:send_request(Node5, fun () -> partisan:node() end),
            try
                partisan_erpc:receive_response(ReqId9),
                ct:fail(unexpected)
            catch
                error:{erpc, notsup} -> ok
            end,

            stop_node(Node5),

            [] = flush([]),

            ok;
        _ ->
            {comment, "No test against OTP 22 node performed"}
    end.


send_request_receive_reqtmo(Config) when is_list(Config) ->
    Fun = fun (Node, SendMe, Timeout) ->
                  RID = partisan_erpc:send_request(Node, erlang, send,
                                          [partisan:self(), SendMe]),
                  try
                      partisan_erpc:receive_response(RID, Timeout),
                      ct:fail(unexpected)
                  catch
                      error:{erpc, timeout} -> ok
                  end
          end,
    reqtmo_test(Config, Fun).

send_request_wait_reqtmo(Config) when is_list(Config) ->
    Fun = fun (Node, SendMe, Timeout) ->
                  RID = partisan_erpc:send_request(Node, erlang, send,
                                          [partisan:self(), SendMe]),
                  no_response = partisan_erpc:wait_response(RID, 0),
                  no_response = partisan_erpc:wait_response(RID, Timeout),
                  %% Cleanup...
                  try
                      partisan_erpc:receive_response(RID, 0),
                      ct:fail(unexpected)
                  catch
                      error:{erpc, timeout} -> ok
                  end
          end,
    reqtmo_test(Config, Fun).

send_request_check_reqtmo(Config) when is_list(Config) ->
    Fun = fun (Node, SendMe, Timeout) ->
                  RID = partisan_erpc:send_request(Node, erlang, send,
                                          [partisan:self(), SendMe]),
                  receive Msg -> partisan_erpc:check_response(Msg, RID)
                  after Timeout -> ok
                  end,
                  %% Cleanup...
                  try
                      partisan_erpc:receive_response(RID, 0),
                      ct:fail(unexpected)
                  catch
                      error:{erpc, timeout} -> ok
                  end
          end,
    reqtmo_test(Config, Fun).

send_request_against_old_node(Config) when is_list(Config) ->
    case start_22_node(Config) of
        {ok, Node22} ->
            RID1 = partisan_erpc:send_request(Node22, partisan, node, []),
            RID2 = partisan_erpc:send_request(Node22, partisan, node, []),
            RID3 = partisan_erpc:send_request(Node22, partisan, node, []),
            try
                partisan_erpc:receive_response(RID1),
                ct:fail(unexpected)
            catch
                error:{erpc, notsup} ->
                    ok
            end,
            try
                partisan_erpc:wait_response(RID2, infinity),
                ct:fail(unexpected)
            catch
                error:{erpc, notsup} ->
                    ok
            end,
            try
                receive
                    Msg ->
                        partisan_erpc:check_response(Msg, RID3),
                        ct:fail(unexpected)
                end
            catch
                error:{erpc, notsup} ->
                    ok
            end,
            stop_node(Node22),
            ok;
        _ ->
        {skipped, "No OTP 22 available"}
    end.

multicall(Config) ->
    {ok, Node1} = start_peer_node(Config),
    {ok, Node2} = start_peer_node(Config),
    {Node3, Node3Res} = case start_22_node(Config) of
                            {ok, N3} ->
                                {N3, {error, {erpc, notsup}}};
                            _ ->
                                {ok, N3} = start_peer_node(Config),
                                stop_node(N3),
                                {N3, {error, {erpc, noconnection}}}
                        end,
    {ok, Node4} = start_peer_node(Config),
    {ok, Node5} = start_peer_node(Config),
    stop_node(Node2),
    io:format("Node1=~p~nNode2=~p~nNode3=~p~nNode4=~p~nNode5=~p~n",
              [Node1, Node2, Node3, Node4, Node5]),

    ThisNode = partisan:node(),
    Nodes = [ThisNode, Node1, Node2, Node3, Node4, Node5],
    
    [{ok, ThisNode},
     {ok, Node1},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, Node4},
     {ok, Node5},
     {error, {erpc, noconnection}}]
        = partisan_erpc:multicall(Nodes ++ [badnodename], partisan, node, []),

    [{ok, ThisNode},
     {ok, Node1},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, Node4},
     {ok, Node5},
     {error, {erpc, noconnection}}]
        = partisan_erpc:multicall(Nodes ++ [badnodename], fun () -> partisan:node() end),

    try
        partisan_erpc:multicall(Nodes ++ [<<"badnodename">>], partisan, node, []),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} ->
            ok
    end,
    
    try
        partisan_erpc:multicall([Node1, Node2, Node3, Node4, Node5 | partisan:node()], partisan, node, []),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} ->
            ok
    end,
    
    
    try
        partisan_erpc:multicall(Nodes ++ [<<"badnodename">>], fun () -> partisan:node() end),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} ->
            ok
    end,

    try
        partisan_erpc:multicall(Nodes, fun (X) -> X end),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} ->
            ok
    end,

    [{ok, ThisNode},
     {ok, Node1},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, Node4},
     {ok, Node5}]
        = partisan_erpc:multicall(Nodes, partisan, node, []),

    [{ok, ThisNode},
     {ok, Node1},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, Node4},
     {ok, Node5}]
        = partisan_erpc:multicall(Nodes, partisan, node, [], 60000),

    [{throw, ThisNode},
     {throw, Node1},
     {error, {erpc, noconnection}},
     Node3Res,
     {throw, Node4},
     {throw, Node5}]
        = partisan_erpc:multicall(Nodes, fun () -> throw(partisan:node()) end),

    [{throw, ThisNode},
     {throw, Node1},
     {error, {erpc, noconnection}},
     Node3Res,
     {throw, Node4},
     {throw, Node5}]
        = partisan_erpc:multicall(Nodes,  fun () -> throw(partisan:node()) end, 60000),

    S0 = erlang:monotonic_time(millisecond),
    [{error, {erpc, timeout}},
     {error, {erpc, timeout}},
     {error, {erpc, noconnection}},
     Node3Res,
     {error, {erpc, timeout}},
     {error, {erpc, timeout}},
     {error, {erpc, timeout}},
     {error, {erpc, timeout}},
     {error, {erpc, noconnection}},
     Node3Res,
     {error, {erpc, timeout}},
     {error, {erpc, timeout}}]
        = partisan_erpc:multicall(Nodes++Nodes, timer, sleep, [2000], 500),
    E0 = erlang:monotonic_time(millisecond),
    T0 = E0 - S0,
    io:format("T0=~p~n", [T0]),
    true = T0 < 1000,

    S1 = erlang:monotonic_time(millisecond),
    [{ok, ok},
     {ok, ok},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, ok},
     {ok, ok},
     {ok, ok},
     {ok, ok},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, ok},
     {ok, ok}]
        = partisan_erpc:multicall(Nodes++Nodes, timer, sleep, [2000]),
    E1 = erlang:monotonic_time(millisecond),
    T1 = E1 - S1,
    io:format("T1=~p~n", [T1]),
    true = T1 < 3000,

    S2 = erlang:monotonic_time(millisecond),
    [{ok, ok},
     {ok, ok},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, ok},
     {ok, ok},
     {ok, ok},
     {ok, ok},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, ok},
     {ok, ok}]
        = partisan_erpc:multicall(Nodes++Nodes, timer, sleep, [2000], 3000),
    E2 = erlang:monotonic_time(millisecond),
    T2 = E2 - S2,
    io:format("T2=~p~n", [T2]),
    true = T2 < 3000,

    [{ok, ThisNode},
     {ok, Node1},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, Node4},
     {ok, Node5},
     {ok, ThisNode},
     {ok, Node1},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, Node4},
     {ok, Node5}]
        = partisan_erpc:multicall(Nodes++Nodes, partisan, node, []),

    [{ok, ThisNode},
     {ok, Node1},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, Node4},
     {ok, Node5},
     {ok, ThisNode},
     {ok, Node1},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, Node4},
     {ok, Node5}]
        = partisan_erpc:multicall(Nodes++Nodes, partisan, node, [], 60000),

    [{ok, ThisNode},
     {ok, Node1},
     {error, {erpc, noconnection}},
     Node3Res,
     {ok, Node4},
     {ok, Node5}]
        = partisan_erpc:multicall(Nodes, fun () -> partisan:node() end),
    
    [BlingError,
     BlingError,
     {error, {erpc, noconnection}},
     Node3Res,
     BlingError,
     BlingError]
        = partisan_erpc:multicall(Nodes, ?MODULE, call_func2, [bling]),

    [BlingError,
     BlingError,
     {error, {erpc, noconnection}},
     Node3Res,
     BlingError,
     BlingError]
        = partisan_erpc:multicall(Nodes, ?MODULE, call_func2, [bling], 60000),

    {error, {exception,
             bling,
             [{?MODULE, call_func3, A, _},
              {?MODULE, call_func2, 1, _}]}} = BlingError,
    true = (A == 1) orelse (A == [bling]),

    [{error, {exception, blong, [{erlang, error, [blong], _}]}},
     {error, {exception, blong, [{erlang, error, [blong], _}]}},
     {error, {erpc, noconnection}},
     Node3Res,
     {error, {exception, blong, [{erlang, error, [blong], _}]}},
     {error, {exception, blong, [{erlang, error, [blong], _}]}}]
        = partisan_erpc:multicall(Nodes, erlang, error, [blong]),

    [{error, {exception, blong, [{erlang, error, [blong], _}]}},
     {error, {exception, blong, [{erlang, error, [blong], _}]}},
     {error, {erpc, noconnection}},
     Node3Res,
     {error, {exception, blong, [{erlang, error, [blong], _}]}},
     {error, {exception, blong, [{erlang, error, [blong], _}]}}]
        = partisan_erpc:multicall(Nodes, erlang, error, [blong], 60000),

    SlowNode4 = fun () ->
                        case partisan:node() of
                            Node4 ->
                                receive after 1000 -> ok end,
                                slow;
                            ThisNode ->
                                throw(fast);
                            _ ->
                                fast
                           end
                end,

    Start1 = erlang:monotonic_time(),
    [{throw, fast},
     {ok, fast},
     {error, {erpc, noconnection}},
     Node3Res,
     {error, {erpc, timeout}},
     {ok, fast}]
        = partisan_erpc:multicall(Nodes, erlang, apply, [SlowNode4, []], 500),

    End1 = erlang:monotonic_time(),

    Time1 = erlang:convert_time_unit(End1-Start1, native, millisecond),
    io:format("Time1 = ~p~n",[Time1]),
    true = Time1 >= 500,
    true = Time1 =< 1000,

    SlowThisNode = fun () ->
                           case partisan:node() of
                               ThisNode ->
                                   receive after 1000 -> ok end,
                                   slow;
                               Node4 ->
                                   throw(fast);
                               Node5 ->
                                   exit(fast);
                               _ ->
                                   fast
                           end
                   end,

    Start2 = erlang:monotonic_time(),
    [{error, {erpc, timeout}},
     {ok, fast},
     {error, {erpc, noconnection}},
     Node3Res,
     {throw, fast},
     {exit, {exception, fast}}] = partisan_erpc:multicall(Nodes, SlowThisNode, 500),

    End2 = erlang:monotonic_time(),

    Time2 = erlang:convert_time_unit(End2-Start2, native, millisecond),
    io:format("Time2 = ~p~n",[Time2]),
    true = Time2 >= 500,
    true = Time2 =< 1000,

    %% We should not get any stray messages due to timed out operations...
    receive Msg -> ct:fail({unexpected, Msg})
    after 1000 -> ok
    end,

    [{error, {erpc, noconnection}},
     Node3Res,
     {error, {erpc, noconnection}},
     {error, {erpc, noconnection}}]
        = partisan_erpc:multicall([Node2, Node3, Node4, Node5], erlang, halt, []),

    [] = flush([]),

    stop_node(Node3),
    case Node3Res of
        {error, {erpc, notsup}} ->
            {comment, "Tested against an OTP 22 node as well"};
        _ ->
            {comment, "No OTP 22 node available; i.e., only testing against current release"}
    end.

multicall_reqtmo(Config) when is_list(Config) ->
    {ok, QuickNode1} = start_node(Config),
    {ok, QuickNode2} = start_node(Config),
    Fun = fun (Node, SendMe, Timeout) ->
                  Me = partisan:self(),
                  ThisNode = partisan:node(),
                  SlowSend = fun () ->
                                     if ThisNode == Node ->
                                             Me ! SendMe,
                                             done;
                                        true ->
                                             done
                                     end
                             end,
                  [{ok, done},{error,{erpc,timeout}},{ok, done}]
                      = partisan_erpc:multicall([QuickNode1, Node, QuickNode2],
                                        erlang, apply, [SlowSend, []],
                                        Timeout)
          end,
    Res = reqtmo_test(Config, Fun),
    stop_node(QuickNode1),
    stop_node(QuickNode2),
    Res.

multicall_recv_opt(Config) when is_list(Config) ->
    Loops = 1000,
    HugeMsgQ = 500000,
    process_flag(message_queue_data, off_heap),
    {ok, Node1} = start_node(Config),
    {ok, Node2} = start_node(Config),
    ExpRes = [{ok, partisan:node()}, {ok, Node1}, {ok, Node2}],
    Nodes = [node(), Node1, Node2],
    Fun = fun () -> partisan:node() end,
    _Warmup = time_multicall(ExpRes, Nodes, Fun, infinity, Loops div 10),
    Empty = time_multicall(ExpRes, Nodes, Fun, infinity, Loops),
    io:format("Time with empty message queue: ~p microsecond~n",
          [erlang:convert_time_unit(Empty, native, microsecond)]),
    _ = [partisan:self() ! {msg,N} || N <- lists:seq(1, HugeMsgQ)],
    Huge = time_multicall(ExpRes, Nodes, Fun, infinity, Loops),
    io:format("Time with huge message queue: ~p microsecond~n",
          [erlang:convert_time_unit(Huge, native, microsecond)]),
    stop_node(Node1),
    stop_node(Node2),
    Q = Huge / Empty,
    HugeMsgQ = flush_msgq(),
    case Q > 10 of
    true ->
        ct:fail({ratio, Q});
    false ->
        {comment, "Ratio: "++erlang:float_to_list(Q)}
    end.

multicall_recv_opt2(Config) when is_list(Config) ->
    Loops = 1000,
    HugeMsgQ = 500000,
    process_flag(message_queue_data, off_heap),
    {ok, Node1} = start_node(Config),
    stop_node(Node1),
    {ok, Node2} = start_node(Config),
    ExpRes = [{ok, partisan:node()}, {error, {erpc, noconnection}}, {ok, Node2}],
    Nodes = [node(), Node1, Node2],
    Fun = fun () -> partisan:node() end,
    _Warmup = time_multicall(ExpRes, Nodes, Fun, infinity, Loops div 10),
    Empty = time_multicall(ExpRes, Nodes, Fun, infinity, Loops),
    io:format("Time with empty message queue: ~p microsecond~n",
          [erlang:convert_time_unit(Empty, native, microsecond)]),
    _ = [partisan:self() ! {msg,N} || N <- lists:seq(1, HugeMsgQ)],
    Huge = time_multicall(ExpRes, Nodes, Fun, infinity, Loops),
    io:format("Time with huge message queue: ~p microsecond~n",
          [erlang:convert_time_unit(Huge, native, microsecond)]),
    stop_node(Node2),
    Q = Huge / Empty,
    HugeMsgQ = flush_msgq(),
    case Q > 10 of
    true ->
        ct:fail({ratio, Q});
    false ->
        {comment, "Ratio: "++erlang:float_to_list(Q)}
    end.

multicall_recv_opt3(Config) when is_list(Config) ->
    Loops = 1000,
    HugeMsgQ = 500000,
    process_flag(message_queue_data, off_heap),
    {ok, Node1} = start_node(Config),
    stop_node(Node1),
    {ok, Node2} = start_node(Config),
    Nodes = [node(), Node1, Node2],
    Fun = fun () -> partisan:node() end,
    _Warmup = time_multicall(undefined, Nodes, Fun, infinity, Loops div 10),
    Empty = time_multicall(undefined, Nodes, Fun, infinity, Loops),
    io:format("Time with empty message queue: ~p microsecond~n",
          [erlang:convert_time_unit(Empty, native, microsecond)]),
    _ = [partisan:self() ! {msg,N} || N <- lists:seq(1, HugeMsgQ)],
    Huge = time_multicall(undefined, Nodes, Fun, 0, Loops),
    io:format("Time with huge message queue: ~p microsecond~n",
          [erlang:convert_time_unit(Huge, native, microsecond)]),
    stop_node(Node2),
    Q = Huge / Empty,
    HugeMsgQ = flush_msgq(),
    case Q > 10 of
    true ->
        ct:fail({ratio, Q});
    false ->
        {comment, "Ratio: "++erlang:float_to_list(Q)}
    end.

time_multicall(Expect, Nodes, Fun, Tmo, Times) ->
    Start = erlang:monotonic_time(),
    ok = do_time_multicall(Expect, Nodes, Fun, Tmo, Times),
    erlang:monotonic_time() - Start.

do_time_multicall(_Expect, _Nodes, _Fun, _Tmo, 0) ->
    ok;
do_time_multicall(undefined, Nodes, Fun, Tmo, N) ->
    _ = partisan_erpc:multicall(Nodes, Fun, Tmo),
    do_time_multicall(undefined, Nodes, Fun, Tmo, N-1);
do_time_multicall(Expect, Nodes, Fun, Tmo, N) ->
    Expect = partisan_erpc:multicall(Nodes, Fun, Tmo),
    do_time_multicall(Expect, Nodes, Fun, Tmo, N-1).

multicast(Config) when is_list(Config) ->
    {ok, Node} = start_node(Config),
    Nodes = [node(), Node],
    try
        partisan_erpc:multicast(Nodes, erlang, send, hej),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} -> ok
    end,
    try
        partisan_erpc:multicast(partisan:node(), erlang, send, [hej]),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} -> ok
    end,
    try
        partisan_erpc:multicast([<<"node">>, Node], erlang, send, [hej]),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} -> ok
    end,
    try
        partisan_erpc:multicast(Nodes, "erlang", send, [hej]),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} -> ok
    end,
    try
        partisan_erpc:multicast(Nodes, erlang, partisan:make_ref(), [hej]),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} -> ok
    end,

    %% Silently fail...
    partisan_erpc:multicast([badnodename], erlang, send, [partisan:self(), blupp]),
    %% silent remote error...
    partisan_erpc:multicast(Nodes, erlang, send, [partisan:self()|blupp]),

    Me = partisan:self(),
    Ok1 = partisan:make_ref(),
    ok = partisan_erpc:multicast(Nodes, erlang, send, [Me, {mfa, Ok1}]),
    receive
        {mfa, Ok1} -> ok
    end,
    receive
        {mfa, Ok1} -> ok
    end,
    ok = partisan_erpc:multicast(Nodes, fun () -> Me ! {a_fun, Ok1} end),
    receive
        {a_fun, Ok1} -> ok
    end,
    receive
        {a_fun, Ok1} -> ok
    end,

    ok = partisan_erpc:multicast([Node], erlang, halt, []),

    monitor_node(Node, true),
    receive {nodedown, Node} -> ok end,

    ok = partisan_erpc:multicast([Node], erlang, send, [Me, wont_reach_me]),

    receive after 1000 -> ok end,

    [] = flush([]),
    case start_22_node(Config) of
        {ok, Node22} ->
            ok = partisan_erpc:multicast([Node], erlang, send, [Me, wont_reach_me]),
            ok = partisan_erpc:multicast([Node], fun () -> Me ! wont_reach_me end),
            receive
                Msg -> ct:fail({unexpected_message, Msg})
            after
                2000 -> ok
            end,
            stop_node(Node22),
            {comment, "Tested against OTP 22 as well"};
        _ ->
            {comment, "No tested against OTP 22"}
    end.

timeout_limit(Config) when is_list(Config) ->
    Node = partisan:node(),
    MaxTmo = (1 bsl 32) - 1,
    erlang:send_after(100, partisan:self(), dummy_message),
    try
        receive
            M ->
                M
        after MaxTmo + 1 ->
                ok
        end,
        ct:fail("The ?MAX_INT_TIMEOUT define in erpc.erl needs "
                "to be updated to reflect max timeout value "
                "in a receive/after...")
    catch
        error:timeout_value ->
            ok
    end,
    Node = partisan_erpc:call(Node, partisan, node, [], MaxTmo),
    try
        partisan_erpc:call(partisan:node(), partisan, node, [], MaxTmo+1),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} ->
            ok
    end,
    [{ok,Node}] = partisan_erpc:multicall([Node], partisan, node, [], MaxTmo),
    try
        partisan_erpc:multicall([Node], partisan, node, [], MaxTmo+1),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} ->
            ok
    end,
    ReqId1 = partisan_erpc:send_request(Node, partisan, node, []),
    Node = partisan_erpc:receive_response(ReqId1, MaxTmo),
    ReqId2 = partisan_erpc:send_request(Node, partisan, node, []),
    try
        partisan_erpc:receive_response(ReqId2, MaxTmo+1),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} ->
            ok
    end,
    ReqId3 = partisan_erpc:send_request(Node, partisan, node, []),
    Node = partisan_erpc:receive_response(ReqId3, MaxTmo),
    ReqId4 = partisan_erpc:send_request(Node, partisan, node, []),
    try
        partisan_erpc:receive_response(ReqId4, MaxTmo+1),
        ct:fail(unexpected)
    catch
        error:{erpc, badarg} ->
            ok
    end,
    ok.
    

%%%
%%% Utility functions.
%%%

start_node(Config) ->
    Name = list_to_atom(atom_to_list(?MODULE)
            ++ "-" ++ atom_to_list(proplists:get_value(testcase, Config))
            ++ "-" ++ integer_to_list(erlang:system_time(second))
            ++ "-" ++ integer_to_list(erlang:unique_integer([positive]))),
    Pa = filename:dirname(code:which(?MODULE)),
    {ok, Node} =
        partisan_support_otp:start_node(Name, [{args,  "-pa " ++ Pa}]),
    ok = partisan_support:cluster(Node),
    timer:sleep(2000),
    {ok, Node}.

start_peer_node(Config) ->
    Name = list_to_atom(atom_to_list(?MODULE)
            ++ "-" ++ atom_to_list(proplists:get_value(testcase, Config))
            ++ "-" ++ integer_to_list(erlang:system_time(second))
            ++ "-" ++ integer_to_list(erlang:unique_integer([positive]))),
    Pa = filename:dirname(code:which(?MODULE)),
    {ok, Node} =
        partisan_support_otp:start_node(Name, [{args,  "-pa " ++ Pa}]),
    ok = partisan_support:cluster(Node),
    timer:sleep(2000),
    {ok, Node}.

start_22_node(_Config) ->
    notsup.

stop_node(Node) ->
    partisan_support_otp:stop_node(Node).

flush(L) ->
    receive
    M ->
        flush([M|L])
    after 0 ->
        L
    end.

t() ->
    [N | _] = string:tokens(atom_to_list(partisan:node()), "_"),
    1000*list_to_integer(N).

f() ->
    timer:sleep(T=t()),
    spawn(?MODULE, f2, []),
    {hej,T,partisan:node()}.

f2() ->
    timer:sleep(500),
    halt().

flush_msgq() ->
    flush_msgq(0).
flush_msgq(N) ->
    receive
    _ ->
        flush_msgq(N+1)
    after 0 ->
        N
    end.