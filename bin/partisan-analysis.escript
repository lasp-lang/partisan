#!/usr/bin/env escript

main([FileToAnalyze]) ->
    io:format("Beginning analysis of: ~p~n", [FileToAnalyze]),

    Analyzer = "src/partisan_analysis", 

    case compile:file(Analyzer, []) of
        {ok, _} ->
            ok;
        _ ->
            io:fwrite("Error: Could not compile analysis.~n", [])
    end,

    CoreForms = case compile:file(FileToAnalyze, [to_core, binary, no_copt]) of
        {ok, _, CFs} ->
            CFs;
        _ ->
            io:fwrite("Error: Could not compile file to analyze. ~n", []),
            exit({error, file_to_analyze_not_loaded})
    end,

    {NewTree, _Max} = cerl_trees:label(CoreForms),

    partisan_analysis:partisan_analysis(NewTree),

    ok.