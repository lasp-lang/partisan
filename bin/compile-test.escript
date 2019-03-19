#!/usr/bin/env escript
%%! -pa ./_build/default/lib/**/ebin -Wall

main([File]) ->
    {ok, _, CoreForms} = compile:file(File, [to_core, binary, no_copt]),

    cerl_trees:fold(fun(Node, _) ->
        Type = cerl:type(Node),

        case Type of 
            'call' ->
                ConcCallName = cerl:concrete(cerl:call_name(Node)),
                io:format("  => ~p~n", [ConcCallName]),
                ok;
            'var' ->
                io:format("=> ~p~n", [cerl:var_name(Node)]),

                case cerl:is_c_fname(Node) of 
                    true ->
                        io:format("=> ~p~n", [Node]),
                        ok;
                    false ->
                        ok
                end;
            _ ->
                io:format("~p~n", [Node]),
                ok
        end
    end, undefined, CoreForms),

    ok.