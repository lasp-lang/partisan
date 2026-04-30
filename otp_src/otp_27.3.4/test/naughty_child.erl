%%
%% A test helper from OTP's lib/stdlib/test/supervisor_SUITE_data
%% Reproduced here because the test data dir wasn't included in the
%% extracted OTP source tarball. Used by supervisor_SUITE:child_unlink/1.
%%
%% A "naughty" child unlinks itself from its supervisor and ignores
%% shutdown signals — exercising the supervisor's brutal-kill path.
%%
-module(naughty_child).
-export([start_link/1]).

start_link(SupPid) ->
    Pid = spawn_link(fun() -> loop() end),
    unlink(Pid),
    unlink(SupPid),
    {ok, Pid}.

loop() ->
    receive _ -> loop() end.
