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

%% -----------------------------------------------------------------------------
%% @doc Mechanical AST rewrite engine for transforming OTP modules (gen_server,
%% gen, etc.) into their Partisan equivalents. Stateless and deterministic.
%%
%% Given the original module name and its parsed forms, produces a new list of
%% forms with module names, remote calls, BIF calls, behaviour attributes, and
%% atoms in data positions rewritten according to the Partisan rename map.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_otp_rewrite).

-export([transform/2]).


%% =============================================================================
%% API
%% =============================================================================


%% @doc Walk the list of forms top-to-bottom, applying all rewrite rules.
transform(OrigModule, Forms) ->
    RenameMap = rename_map(),
    PartisanModule = maps:get(OrigModule, RenameMap),
    lists:filtermap(
        fun(Form) ->
            transform_form(Form, OrigModule, PartisanModule, RenameMap)
        end,
        Forms
    ).


%% =============================================================================
%% INTERNAL: Rename maps
%% =============================================================================


%% Module rename map used for atom-in-data renaming, module attribute, and
%% behaviour attribute rewrites.
rename_map() ->
    #{
        gen_server => partisan_gen_server,
        gen => partisan_gen,
        gen_event => partisan_gen_event,
        gen_fsm => partisan_gen_fsm,
        gen_statem => partisan_gen_statem,
        supervisor => partisan_gen_supervisor,
        proc_lib => partisan_proc_lib,
        sys => partisan_sys
    }.


%% Remote call rename map. Includes everything in rename_map/0 plus rpc.
call_rename_map() ->
    #{
        gen_server => partisan_gen_server,
        gen => partisan_gen,
        gen_event => partisan_gen_event,
        gen_fsm => partisan_gen_fsm,
        gen_statem => partisan_gen_statem,
        supervisor => partisan_gen_supervisor,
        proc_lib => partisan_proc_lib,
        sys => partisan_sys,
        rpc => partisan_rpc
    }.


%% erlang:Fun calls that should be rewritten to partisan:Fun.
erlang_bif_rewrites() ->
    #{
        monitor => partisan,
        demonitor => partisan
    }.


%% Auto-imported BIFs that should be rewritten to partisan:Fun in body context.
%% monitor/2,3 and demonitor/1,2 became auto-imports in OTP 26; OTP 28 beam
%% files contain them as bare calls rather than erlang:monitor/erlang:demonitor.
auto_import_rewrites() ->
    #{
        {node, 0} => partisan,
        {node, 1} => partisan,
        {monitor_node, 2} => partisan,
        {make_ref, 0} => partisan,
        {monitor, 2} => partisan,
        {monitor, 3} => partisan,
        {demonitor, 1} => partisan,
        {demonitor, 2} => partisan,
        {is_pid, 1} => partisan,
        {is_process_alive, 1} => partisan,
        {exit, 2} => partisan
    }.


%% =============================================================================
%% INTERNAL: Form-level transform
%% =============================================================================


%% Module attribute: rename to Partisan equivalent.
transform_form({attribute, Anno, module, _Mod}, _OrigModule, PartisanModule, _Map) ->
    {true, {attribute, Anno, module, PartisanModule}};

%% Strip all spec attributes.
transform_form({attribute, _Anno, spec, _}, _, _, _) ->
    false;

%% Behaviour attribute: rename if the behaviour is in the rename map.
transform_form({attribute, Anno, behaviour, Behav}, _, _, Map) ->
    case maps:find(Behav, Map) of
        {ok, NewBehav} -> {true, {attribute, Anno, behaviour, NewBehav}};
        error -> true
    end;

%% Also handle the US English spelling.
transform_form({attribute, Anno, behavior, Behav}, _, _, Map) ->
    case maps:find(Behav, Map) of
        {ok, NewBehav} -> {true, {attribute, Anno, behavior, NewBehav}};
        error -> true
    end;

%% Function declarations: recurse into clauses.
transform_form({function, Anno, Name, Arity, Clauses}, _, _, Map) ->
    NewClauses = [transform_clause(C, Map) || C <- Clauses],
    {true, {function, Anno, Name, Arity, NewClauses}};

%% All other forms (imports, exports, records, types, callbacks, etc.) kept as-is.
transform_form(_Form, _, _, _) ->
    true.


%% =============================================================================
%% INTERNAL: Clause transform
%% =============================================================================


transform_clause({clause, Anno, Patterns, Guards, Body}, Map) ->
    NewPatterns = [transform_expr(P, Map, pattern) || P <- Patterns],
    NewGuards = [transform_guard_seq(G, Map) || G <- Guards],
    NewBody = [transform_expr(E, Map, body) || E <- Body],
    {clause, Anno, NewPatterns, NewGuards, NewBody}.


%% Guard sequences are lists of guard tests.
transform_guard_seq(Guards, Map) ->
    [transform_expr(G, Map, guard) || G <- Guards].


%% =============================================================================
%% INTERNAL: Expression transform
%%
%% Context is one of: body | guard | pattern
%%   body    - full rewrite including auto-import BIFs
%%   guard   - rewrite remote calls and atoms, but NOT auto-import BIFs
%%   pattern - rewrite atoms in data positions only
%% =============================================================================


%% -- Remote call to erlang module: check BIF rewrites -------------------------
transform_expr(
    {call, Anno,
        {remote, Anno2, {atom, Anno3, erlang}, {atom, Anno4, Fun}}, Args},
    Map, Ctx
) when Ctx =:= body; Ctx =:= guard ->
    BifRewrites = erlang_bif_rewrites(),
    NewArgs = [transform_expr(A, Map, Ctx) || A <- Args],
    case maps:find(Fun, BifRewrites) of
        {ok, NewMod} ->
            {call, Anno,
                {remote, Anno2, {atom, Anno3, NewMod}, {atom, Anno4, Fun}},
                NewArgs};
        error ->
            {call, Anno,
                {remote, Anno2, {atom, Anno3, erlang}, {atom, Anno4, Fun}},
                NewArgs}
    end;

%% -- Remote call to a module in the call rename map ---------------------------
transform_expr(
    {call, Anno,
        {remote, Anno2, {atom, Anno3, Mod}, {atom, Anno4, Fun}}, Args},
    Map, Ctx
) when Ctx =:= body; Ctx =:= guard ->
    CallMap = call_rename_map(),
    NewMod = maps:get(Mod, CallMap, Mod),
    NewArgs = [transform_expr(A, Map, Ctx) || A <- Args],
    {call, Anno,
        {remote, Anno2, {atom, Anno3, NewMod}, {atom, Anno4, Fun}},
        NewArgs};

%% -- Remote call with non-literal module or function --------------------------
transform_expr(
    {call, Anno, {remote, Anno2, ModExpr, FunExpr}, Args},
    Map, Ctx
) when Ctx =:= body; Ctx =:= guard ->
    NewMod = transform_expr(ModExpr, Map, Ctx),
    NewFun = transform_expr(FunExpr, Map, Ctx),
    NewArgs = [transform_expr(A, Map, Ctx) || A <- Args],
    {call, Anno, {remote, Anno2, NewMod, NewFun}, NewArgs};

%% -- Local call in body context: check auto-import rewrites -------------------
transform_expr(
    {call, Anno, {atom, Anno2, Fun}, Args},
    Map, body
) ->
    AutoImports = auto_import_rewrites(),
    Arity = length(Args),
    NewArgs = [transform_expr(A, Map, body) || A <- Args],
    case maps:find({Fun, Arity}, AutoImports) of
        {ok, NewMod} ->
            {call, Anno,
                {remote, Anno, {atom, Anno2, NewMod}, {atom, Anno2, Fun}},
                NewArgs};
        error ->
            {call, Anno, {atom, Anno2, Fun}, NewArgs}
    end;

%% -- Local call in guard context: no auto-import rewrite ----------------------
transform_expr(
    {call, Anno, {atom, Anno2, Fun}, Args},
    Map, guard
) ->
    NewArgs = [transform_expr(A, Map, guard) || A <- Args],
    {call, Anno, {atom, Anno2, Fun}, NewArgs};

%% -- Local call with non-literal fun expression -------------------------------
transform_expr({call, Anno, FunExpr, Args}, Map, Ctx)
        when Ctx =:= body; Ctx =:= guard ->
    NewFun = transform_expr(FunExpr, Map, Ctx),
    NewArgs = [transform_expr(A, Map, Ctx) || A <- Args],
    {call, Anno, NewFun, NewArgs};

%% -- Fun reference: remote (module:fun/arity) ---------------------------------
transform_expr(
    {'fun', Anno,
        {function, {atom, Anno2, Mod}, {atom, Anno3, Fun}, Arity}},
    _Map, _Ctx
) ->
    CallMap = call_rename_map(),
    NewMod = maps:get(Mod, CallMap, Mod),
    {'fun', Anno,
        {function, {atom, Anno2, NewMod}, {atom, Anno3, Fun}, Arity}};

%% -- Fun reference: local (fun name/arity) ------------------------------------
transform_expr({'fun', Anno, {function, Name, Arity}}, _Map, _Ctx) ->
    {'fun', Anno, {function, Name, Arity}};

%% -- Fun with clauses (fun(...) -> ... end) -----------------------------------
transform_expr({'fun', Anno, {clauses, Clauses}}, Map, _Ctx) ->
    NewClauses = [transform_clause(C, Map) || C <- Clauses],
    {'fun', Anno, {clauses, NewClauses}};

%% -- Named fun ----------------------------------------------------------------
transform_expr({named_fun, Anno, Name, Clauses}, Map, _Ctx) ->
    NewClauses = [transform_clause(C, Map) || C <- Clauses],
    {named_fun, Anno, Name, NewClauses};

%% -- Atom in data position: rename if key in rename map -----------------------
transform_expr({atom, Anno, Val}, Map, _Ctx) ->
    case maps:find(Val, Map) of
        {ok, NewVal} -> {atom, Anno, NewVal};
        error -> {atom, Anno, Val}
    end;

%% -- Leaf nodes: return as-is -------------------------------------------------
transform_expr({integer, _, _} = Node, _Map, _Ctx) -> Node;
transform_expr({float, _, _} = Node, _Map, _Ctx) -> Node;
transform_expr({string, _, _} = Node, _Map, _Ctx) -> Node;
transform_expr({char, _, _} = Node, _Map, _Ctx) -> Node;
transform_expr({var, _, _} = Node, _Map, _Ctx) -> Node;
transform_expr({nil, _} = Node, _Map, _Ctx) -> Node;

%% -- Compound expressions: recurse -------------------------------------------

%% Cons cell
transform_expr({cons, Anno, H, T}, Map, Ctx) ->
    {cons, Anno, transform_expr(H, Map, Ctx), transform_expr(T, Map, Ctx)};

%% Tuple
transform_expr({tuple, Anno, Es}, Map, Ctx) ->
    {tuple, Anno, [transform_expr(E, Map, Ctx) || E <- Es]};

%% Binary
transform_expr({bin, Anno, BinElements}, Map, Ctx) ->
    {bin, Anno, [transform_bin_element(BE, Map, Ctx) || BE <- BinElements]};

%% Map creation
transform_expr({map, Anno, Assocs}, Map, Ctx) ->
    {map, Anno, [transform_expr(A, Map, Ctx) || A <- Assocs]};

%% Map update
transform_expr({map, Anno, Expr, Assocs}, Map, Ctx) ->
    {map, Anno,
        transform_expr(Expr, Map, Ctx),
        [transform_expr(A, Map, Ctx) || A <- Assocs]};

%% Map field association
transform_expr({map_field_assoc, Anno, K, V}, Map, Ctx) ->
    {map_field_assoc, Anno,
        transform_expr(K, Map, Ctx),
        transform_expr(V, Map, Ctx)};

%% Map field exact
transform_expr({map_field_exact, Anno, K, V}, Map, Ctx) ->
    {map_field_exact, Anno,
        transform_expr(K, Map, Ctx),
        transform_expr(V, Map, Ctx)};

%% Record creation
transform_expr({record, Anno, Name, Fields}, Map, Ctx) ->
    {record, Anno, Name,
        [transform_expr(F, Map, Ctx) || F <- Fields]};

%% Record update
transform_expr({record, Anno, Expr, Name, Fields}, Map, Ctx) ->
    {record, Anno,
        transform_expr(Expr, Map, Ctx),
        Name,
        [transform_expr(F, Map, Ctx) || F <- Fields]};

%% Record field
transform_expr({record_field, Anno, Name, Val}, Map, Ctx) ->
    {record_field, Anno, Name, transform_expr(Val, Map, Ctx)};

%% Record index
transform_expr({record_index, _, _, _} = Node, _Map, _Ctx) ->
    Node;

%% Record field access
transform_expr({record_field, Anno, Expr, RecName, FieldName}, Map, Ctx) ->
    {record_field, Anno, transform_expr(Expr, Map, Ctx), RecName, FieldName};

%% Block
transform_expr({block, Anno, Body}, Map, Ctx) ->
    {block, Anno, [transform_expr(E, Map, Ctx) || E <- Body]};

%% If
transform_expr({'if', Anno, Clauses}, Map, _Ctx) ->
    {'if', Anno, [transform_clause(C, Map) || C <- Clauses]};

%% Case
transform_expr({'case', Anno, Expr, Clauses}, Map, Ctx) ->
    {'case', Anno,
        transform_expr(Expr, Map, Ctx),
        [transform_clause(C, Map) || C <- Clauses]};

%% Try-catch
transform_expr({'try', Anno, Body, Cases, Catches, After}, Map, _Ctx) ->
    {'try', Anno,
        [transform_expr(E, Map, body) || E <- Body],
        [transform_clause(C, Map) || C <- Cases],
        [transform_clause(C, Map) || C <- Catches],
        [transform_expr(E, Map, body) || E <- After]};

%% Receive
transform_expr({'receive', Anno, Clauses}, Map, _Ctx) ->
    {'receive', Anno, [transform_clause(C, Map) || C <- Clauses]};

%% Receive with timeout
transform_expr({'receive', Anno, Clauses, Timeout, After}, Map, _Ctx) ->
    {'receive', Anno,
        [transform_clause(C, Map) || C <- Clauses],
        transform_expr(Timeout, Map, body),
        [transform_expr(E, Map, body) || E <- After]};

%% Match
transform_expr({match, Anno, P, E}, Map, Ctx) ->
    {match, Anno,
        transform_expr(P, Map, pattern),
        transform_expr(E, Map, Ctx)};

%% Binary operator
transform_expr({op, Anno, Op, L, R}, Map, Ctx) ->
    {op, Anno, Op,
        transform_expr(L, Map, Ctx),
        transform_expr(R, Map, Ctx)};

%% Unary operator
transform_expr({op, Anno, Op, E}, Map, Ctx) ->
    {op, Anno, Op, transform_expr(E, Map, Ctx)};

%% Catch
transform_expr({'catch', Anno, E}, Map, Ctx) ->
    {'catch', Anno, transform_expr(E, Map, Ctx)};

%% List comprehension
transform_expr({lc, Anno, E, Qs}, Map, Ctx) ->
    {lc, Anno,
        transform_expr(E, Map, Ctx),
        [transform_qualifier(Q, Map, Ctx) || Q <- Qs]};

%% Binary comprehension
transform_expr({bc, Anno, E, Qs}, Map, Ctx) ->
    {bc, Anno,
        transform_expr(E, Map, Ctx),
        [transform_qualifier(Q, Map, Ctx) || Q <- Qs]};

%% Map comprehension (OTP 26+)
transform_expr({mc, Anno, E, Qs}, Map, Ctx) ->
    {mc, Anno,
        transform_expr(E, Map, Ctx),
        [transform_qualifier(Q, Map, Ctx) || Q <- Qs]};

%% Maybe expression (OTP 25+)
transform_expr({'maybe', Anno, Body}, Map, _Ctx) ->
    {'maybe', Anno, [transform_expr(E, Map, body) || E <- Body]};

transform_expr({'maybe', Anno, Body, {'else', ElseAnno, Clauses}}, Map, _Ctx) ->
    {'maybe', Anno,
        [transform_expr(E, Map, body) || E <- Body],
        {'else', ElseAnno, [transform_clause(C, Map) || C <- Clauses]}};

%% Maybe match (?=)
transform_expr({maybe_match, Anno, P, E}, Map, Ctx) ->
    {maybe_match, Anno,
        transform_expr(P, Map, pattern),
        transform_expr(E, Map, Ctx)};

%% Catch-all: return unrecognized nodes unchanged.
transform_expr(Other, _Map, _Ctx) ->
    Other.


%% =============================================================================
%% INTERNAL: Helpers
%% =============================================================================


%% Binary element
transform_bin_element({bin_element, Anno, Expr, Size, TSL}, Map, Ctx) ->
    NewExpr = transform_expr(Expr, Map, Ctx),
    NewSize = case Size of
        default -> default;
        _ -> transform_expr(Size, Map, Ctx)
    end,
    {bin_element, Anno, NewExpr, NewSize, TSL}.


%% Comprehension qualifiers
transform_qualifier({generate, Anno, P, E}, Map, Ctx) ->
    {generate, Anno,
        transform_expr(P, Map, pattern),
        transform_expr(E, Map, Ctx)};

transform_qualifier({b_generate, Anno, P, E}, Map, Ctx) ->
    {b_generate, Anno,
        transform_expr(P, Map, pattern),
        transform_expr(E, Map, Ctx)};

transform_qualifier({m_generate, Anno, P, E}, Map, Ctx) ->
    {m_generate, Anno,
        transform_expr(P, Map, pattern),
        transform_expr(E, Map, Ctx)};

transform_qualifier(Filter, Map, Ctx) ->
    transform_expr(Filter, Map, Ctx).
