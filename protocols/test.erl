-module(test).

one() ->
	1.

two() ->
	self() ! one.

three() ->
	erlang:apply(something).

four({_, _}) -> 
	one();
four(_) -> 
	two().