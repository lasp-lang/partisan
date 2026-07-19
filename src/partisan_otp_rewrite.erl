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
    Rewritten = lists:filtermap(
        fun(Form) ->
            transform_form(Form, OrigModule, PartisanModule, RenameMap)
        end,
        Forms
    ),
    lift_is_pid_guards(OrigModule, Rewritten).

%% =============================================================================
%% INTERNAL: is_pid guard lifting (supervisor only)
%%
%% Erlang guards cannot call `partisan:is_pid/1', so OTP supervisor functions
%% that select a clause with an `is_pid/1' guard cannot be used verbatim for
%% Partisan (whose pids may be remote references). Rather than hand-copy such
%% functions from OTP source into `partisan_otp_patches' (which re-introduces
%% the "OTP N+1 silently breaks our frozen fork" bug class), we mechanically
%% lift the guard into a body-level `partisan:is_pid/1' check so the function
%% body still comes from the installed OTP source.
%%
%% The transform is deliberately NARROW and only fires on a provably-sound
%% shape: a two-clause function whose first clause has irrefutable (all
%% variable) patterns and a single `is_pid(Expr)' guard, and whose second
%% clause is an irrefutable, unguarded catch-all. For such a function the ONLY
%% thing that selects between the two clauses is the guard, so:
%%
%%     f(P1..) when is_pid(E) -> Body1;
%%     f(P2..)               -> Body2.
%%
%% is exactly equivalent to:
%%
%%     f(P1..) ->
%%         case partisan:is_pid(E) of
%%             true  -> Body1;
%%             false -> Body2'    %% Body2 with P2 vars renamed to P1 vars
%%         end.
%%
%% (Both heads are irrefutable, so C1's head accepts every value C2's would;
%% the guard is the sole discriminator, and its failure falls through to C2.)
%% Scoped to `supervisor' only to keep the blast radius minimal. Any supervisor
%% function that does NOT match this exact shape is left untouched and stays
%% covered by an explicit patch in `partisan_otp_patches'.
%% =============================================================================

lift_is_pid_guards(supervisor, Forms) ->
    [lift_form(F) || F <- Forms];
lift_is_pid_guards(_OrigModule, Forms) ->
    Forms.

lift_form({function, Anno, Name, Arity, Clauses} = Form) ->
    case liftable_clauses(Clauses) of
        {true, Lifted} -> {function, Anno, Name, Arity, [Lifted]};
        false -> Form
    end;
lift_form(Form) ->
    Form.

%% Recognise the narrow, provably-sound two-clause shape and build the lifted
%% single clause. Returns {true, Clause} or false.
liftable_clauses([
    {clause, A1, P1s, [[{call, GA, {atom, _, is_pid}, [Arg]}]], Body1},
    {clause, _A2, P2s, [], Body2}
]) ->
    case all_vars(P1s) andalso all_vars(P2s) of
        true ->
            Renames = var_renames(P2s, P1s),
            case capture_safe(Renames, Body2) of
                true ->
                    Body2r = rename_vars(Body2, Renames),
                    IsPid =
                        {call, GA,
                            {remote, GA, {atom, GA, partisan},
                                {atom, GA, is_pid}},
                            [Arg]},
                    Case =
                        {'case', A1, IsPid, [
                            {clause, A1, [{atom, A1, true}], [], Body1},
                            {clause, A1, [{atom, A1, false}], [], Body2r}
                        ]},
                    {true, {clause, A1, P1s, [], [Case]}};
                false ->
                    %% Renaming would introduce a guarded-clause head variable
                    %% name that is already bound inside the catch-all body,
                    %% fusing two distinct bindings into one (variable capture
                    %% -> miscompile). Refuse to lift and leave the function
                    %% untouched; if the lift was actually required, the
                    %% fail-loud build guard in `partisan_otp_patches'
                    %% (assert_is_pid_guards_lifted/2) flags it loudly rather
                    %% than letting a native `is_pid/1' guard survive silently.
                    false
            end;
        false ->
            false
    end;
liftable_clauses(_Clauses) ->
    false.

all_vars(Patterns) ->
    lists:all(
        fun
            ({var, _, _}) -> true;
            (_) -> false
        end,
        Patterns
    ).

%% Build a variable rename map from the catch-all clause's positional variables
%% (which its body may reference) to the guarded clause's positional variables
%% (which become the sole clause head). Underscore/anonymous and identical
%% names are skipped.
var_renames(FromPatterns, ToPatterns) ->
    lists:foldl(
        fun
            ({{var, _, From}, {var, _, To}}, Acc) when
                From =/= To, From =/= '_', To =/= '_'
            ->
                Acc#{From => To};
            (_, Acc) ->
                Acc
        end,
        #{},
        lists:zip(FromPatterns, ToPatterns)
    ).

%% Rename variable occurrences according to Renames. Applied to the catch-all
%% body only, whose free variables are all bound by the (irrefutable) clause
%% head we are collapsing into.
rename_vars({var, Anno, Name}, Renames) ->
    {var, Anno, maps:get(Name, Renames, Name)};
rename_vars(T, Renames) when is_tuple(T) ->
    list_to_tuple([rename_vars(E, Renames) || E <- tuple_to_list(T)]);
rename_vars(L, Renames) when is_list(L) ->
    [rename_vars(E, Renames) || E <- L];
rename_vars(Other, _Renames) ->
    Other.

%% A lift is capture-safe only if none of the variable names we are about to
%% introduce into the catch-all body (the *range* of the rename map — the
%% guarded clause's head vars) already occurs as a variable in that body. If
%% one does, the uniform rename fuses two distinct bindings into a single
%% variable and changes the function's meaning (e.g. `X = A + 1' under a rename
%% A -> X becomes the self-referential `X = X + 1'). We check ALL range names
%% (not only those whose source occurs in the body) so the guard is
%% conservative: a false "unsafe" merely leaves the function unlifted (caught,
%% if needed, by the fail-loud assert), whereas a false "safe" miscompiles.
capture_safe(Renames, Body2) ->
    Introduced = maps:values(Renames),
    BodyVars = body_var_names(Body2),
    not lists:any(fun(V) -> lists:member(V, BodyVars) end, Introduced).

%% Collect the set (as a deduped list) of every variable NAME occurring
%% anywhere in an AST fragment — heads, nested binds, comprehensions, funs.
%% Anonymous `_' is ignored. Deliberately scope-blind: over-collecting can only
%% make `capture_safe/2' refuse a lift, never wrongly approve one.
body_var_names(AST) ->
    body_var_names(AST, []).

body_var_names({var, _, '_'}, Acc) ->
    Acc;
body_var_names({var, _, Name}, Acc) ->
    case lists:member(Name, Acc) of
        true -> Acc;
        false -> [Name | Acc]
    end;
body_var_names(T, Acc) when is_tuple(T) ->
    body_var_names(tuple_to_list(T), Acc);
body_var_names([H | T], Acc) ->
    body_var_names(T, body_var_names(H, Acc));
body_var_names(_Other, Acc) ->
    Acc.

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
        %% NOTE: make_ref/0 is intentionally NOT rewritten. gen_statem uses
        %% synthetic `make_ref()` values as timer tags that are passed to
        %% erlang:cancel_timer/1, which only accepts native references.
        %% gen_server uses make_ref/0 for internal state tags as well. Partisan
        %% ref encoding happens at the transport boundary (send/receive), so
        %% internal refs can remain native.
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
transform_form(
    {attribute, Anno, module, _Mod}, _OrigModule, PartisanModule, _Map
) ->
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
    {call, Anno, {remote, Anno2, {atom, Anno3, erlang}, {atom, Anno4, Fun}},
        Args},
    Map,
    Ctx
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
    {call, Anno, {remote, Anno2, {atom, Anno3, Mod}, {atom, Anno4, Fun}}, Args},
    Map,
    Ctx
) when Ctx =:= body; Ctx =:= guard ->
    CallMap = call_rename_map(),
    NewMod = maps:get(Mod, CallMap, Mod),
    NewArgs = [transform_expr(A, Map, Ctx) || A <- Args],
    {call, Anno, {remote, Anno2, {atom, Anno3, NewMod}, {atom, Anno4, Fun}},
        NewArgs};
%% -- Remote call with non-literal module or function --------------------------
transform_expr(
    {call, Anno, {remote, Anno2, ModExpr, FunExpr}, Args},
    Map,
    Ctx
) when Ctx =:= body; Ctx =:= guard ->
    NewMod = transform_expr(ModExpr, Map, Ctx),
    NewFun = transform_expr(FunExpr, Map, Ctx),
    NewArgs = [transform_expr(A, Map, Ctx) || A <- Args],
    {call, Anno, {remote, Anno2, NewMod, NewFun}, NewArgs};
%% -- Local call in body context: check auto-import rewrites -------------------
transform_expr(
    {call, Anno, {atom, Anno2, Fun}, Args},
    Map,
    body
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
    Map,
    guard
) ->
    NewArgs = [transform_expr(A, Map, guard) || A <- Args],
    {call, Anno, {atom, Anno2, Fun}, NewArgs};
%% -- Local call with non-literal fun expression -------------------------------
transform_expr({call, Anno, FunExpr, Args}, Map, Ctx) when
    Ctx =:= body; Ctx =:= guard
->
    NewFun = transform_expr(FunExpr, Map, Ctx),
    NewArgs = [transform_expr(A, Map, Ctx) || A <- Args],
    {call, Anno, NewFun, NewArgs};
%% -- Fun reference: remote (module:fun/arity) ---------------------------------
transform_expr(
    {'fun', Anno, {function, {atom, Anno2, Mod}, {atom, Anno3, Fun}, Arity}},
    _Map,
    _Ctx
) ->
    CallMap = call_rename_map(),
    NewMod = maps:get(Mod, CallMap, Mod),
    {'fun', Anno, {function, {atom, Anno2, NewMod}, {atom, Anno3, Fun}, Arity}};
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
transform_expr({integer, _, _} = Node, _Map, _Ctx) ->
    Node;
transform_expr({float, _, _} = Node, _Map, _Ctx) ->
    Node;
transform_expr({string, _, _} = Node, _Map, _Ctx) ->
    Node;
transform_expr({char, _, _} = Node, _Map, _Ctx) ->
    Node;
transform_expr({var, _, _} = Node, _Map, _Ctx) ->
    Node;
transform_expr({nil, _} = Node, _Map, _Ctx) ->
    Node;
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
    {map, Anno, transform_expr(Expr, Map, Ctx), [
        transform_expr(A, Map, Ctx)
     || A <- Assocs
    ]};
%% Map field association
transform_expr({map_field_assoc, Anno, K, V}, Map, Ctx) ->
    {map_field_assoc, Anno, transform_expr(K, Map, Ctx),
        transform_expr(V, Map, Ctx)};
%% Map field exact
transform_expr({map_field_exact, Anno, K, V}, Map, Ctx) ->
    {map_field_exact, Anno, transform_expr(K, Map, Ctx),
        transform_expr(V, Map, Ctx)};
%% Record creation
transform_expr({record, Anno, Name, Fields}, Map, Ctx) ->
    {record, Anno, Name, [transform_expr(F, Map, Ctx) || F <- Fields]};
%% Record update
transform_expr({record, Anno, Expr, Name, Fields}, Map, Ctx) ->
    {record, Anno, transform_expr(Expr, Map, Ctx), Name, [
        transform_expr(F, Map, Ctx)
     || F <- Fields
    ]};
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
    {'case', Anno, transform_expr(Expr, Map, Ctx), [
        transform_clause(C, Map)
     || C <- Clauses
    ]};
%% Try-catch
transform_expr({'try', Anno, Body, Cases, Catches, After}, Map, _Ctx) ->
    {'try', Anno, [transform_expr(E, Map, body) || E <- Body],
        [transform_clause(C, Map) || C <- Cases],
        [transform_clause(C, Map) || C <- Catches], [
            transform_expr(E, Map, body)
         || E <- After
        ]};
%% Receive
transform_expr({'receive', Anno, Clauses}, Map, _Ctx) ->
    {'receive', Anno, [transform_clause(C, Map) || C <- Clauses]};
%% Receive with timeout
transform_expr({'receive', Anno, Clauses, Timeout, After}, Map, _Ctx) ->
    {'receive', Anno, [transform_clause(C, Map) || C <- Clauses],
        transform_expr(Timeout, Map, body), [
            transform_expr(E, Map, body)
         || E <- After
        ]};
%% Match
transform_expr({match, Anno, P, E}, Map, Ctx) ->
    {match, Anno, transform_expr(P, Map, pattern), transform_expr(E, Map, Ctx)};
%% Binary operator
transform_expr({op, Anno, Op, L, R}, Map, Ctx) ->
    {op, Anno, Op, transform_expr(L, Map, Ctx), transform_expr(R, Map, Ctx)};
%% Unary operator
transform_expr({op, Anno, Op, E}, Map, Ctx) ->
    {op, Anno, Op, transform_expr(E, Map, Ctx)};
%% Catch
transform_expr({'catch', Anno, E}, Map, Ctx) ->
    {'catch', Anno, transform_expr(E, Map, Ctx)};
%% List comprehension
transform_expr({lc, Anno, E, Qs}, Map, Ctx) ->
    {lc, Anno, transform_expr(E, Map, Ctx), [
        transform_qualifier(Q, Map, Ctx)
     || Q <- Qs
    ]};
%% Binary comprehension
transform_expr({bc, Anno, E, Qs}, Map, Ctx) ->
    {bc, Anno, transform_expr(E, Map, Ctx), [
        transform_qualifier(Q, Map, Ctx)
     || Q <- Qs
    ]};
%% Map comprehension (OTP 26+)
transform_expr({mc, Anno, E, Qs}, Map, Ctx) ->
    {mc, Anno, transform_expr(E, Map, Ctx), [
        transform_qualifier(Q, Map, Ctx)
     || Q <- Qs
    ]};
%% Maybe expression (OTP 25+)
transform_expr({'maybe', Anno, Body}, Map, _Ctx) ->
    {'maybe', Anno, [transform_expr(E, Map, body) || E <- Body]};
transform_expr({'maybe', Anno, Body, {'else', ElseAnno, Clauses}}, Map, _Ctx) ->
    {'maybe', Anno, [transform_expr(E, Map, body) || E <- Body],
        {'else', ElseAnno, [transform_clause(C, Map) || C <- Clauses]}};
%% Maybe match (?=)
transform_expr({maybe_match, Anno, P, E}, Map, Ctx) ->
    {maybe_match, Anno, transform_expr(P, Map, pattern),
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
    NewSize =
        case Size of
            default -> default;
            _ -> transform_expr(Size, Map, Ctx)
        end,
    {bin_element, Anno, NewExpr, NewSize, TSL}.

%% Comprehension qualifiers
transform_qualifier({generate, Anno, P, E}, Map, Ctx) ->
    {generate, Anno, transform_expr(P, Map, pattern),
        transform_expr(E, Map, Ctx)};
transform_qualifier({b_generate, Anno, P, E}, Map, Ctx) ->
    {b_generate, Anno, transform_expr(P, Map, pattern),
        transform_expr(E, Map, Ctx)};
transform_qualifier({m_generate, Anno, P, E}, Map, Ctx) ->
    {m_generate, Anno, transform_expr(P, Map, pattern),
        transform_expr(E, Map, Ctx)};
transform_qualifier(Filter, Map, Ctx) ->
    transform_expr(Filter, Map, Ctx).
