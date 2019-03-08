#!/usr/bin/env escript

main([]) ->
    Analyzer = "cerl_closurean_modified", 

    case compile:file(Analyzer, []) of
        {ok, _} ->
            ok;
        _ ->
            io:fwrite("Error: Could not compile analysis.~n", [])
    end,

    FileToAnalyze = "lampson_2pc", 

    CoreForms = case compile:file(FileToAnalyze, [to_core, binary, no_copt]) of
        {ok, _, CFs} ->
            CFs;
        _ ->
            io:fwrite("Error: Could not compile file to analyze. ~n", []),
            exit({error, file_to_analyze_not_loaded})
    end,

    {NewTree, _Max} = cerl_trees:label(CoreForms),

    {AnnotatedTree, _, _, _, _, _} = cerl_closurean_modified:annotate(NewTree),

    F = fun(T) ->
		case cerl:type(T) of
		    'fun' ->
                io:format("Abstraction: ~p~n", [cerl:get_ann(T)]),
                T;
		    apply ->
                io:format("Application: ~p~n", [cerl:get_ann(T)]),
                T;
		    _ ->
                T
		end
    end,

    cerl_trees:map(F, AnnotatedTree),

    ok.