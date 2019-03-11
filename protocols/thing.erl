-module(thing).
-export([init/0, thing/0, otherthing/0, finalthing/0]).

init() ->
    ok.

thing() ->
    1,
    Fun1 = fun() -> otherthing() end,
    Fun1(),

    Fun2 = fun() -> finalthing() end,
    Fun2(),

    lists:foreach(fun(X) -> X end, finalthing()),

    finalthing(),
    rand:seed().

otherthing() ->
    ok.

finalthing() ->
    [].