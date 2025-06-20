%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2025 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(partisan_retry).

-include("partisan.hrl").

?MODULEDOC("""
# Overview

This module implements a structured retry mechanism with optional exponential
backoff and deadline tracking, designed for reliable asynchronous retries.
It wraps the [Backoff](https://hex.pm/packages/backoff) library.

## Key Features
- Supports a fixed retry interval or exponential backoff.
- Tracks retry attempts (`count`) and enforces a maximum retry limit.
- Optional global deadline (in milliseconds) for all retries.
- Integrates with `erlang:start_timer/3` to fire retries after delay.

## Record Structure
- `#partisan_retry{}` contains retry state, including:
- `id`              : Identifier associated with the retry (e.g., message ID).
- `deadline`        : Absolute time limit in ms (from start) before giving up.
- `max_retries`     : Maximum allowed retry attempts.
- `interval`        : Default interval between retries (in ms).
- `count`           : Current number of failed attempts.
- `backoff`         : Optional backoff state (using external `backoff` module).
- `start_ts`        : Start timestamp (used for deadline tracking).

## Retry Configuration Options (init/2)
Can be supplied as a proplist or map:
- `{deadline, pos_integer()}` - Absolute time limit in milliseconds for all retries.
- `{max_retries, pos_integer()}` - Max number of retry attempts.
- `backoff`
    - `{enabled, boolean()}`  - Enables exponential backoff.
    - `{min, pos_integer()}` - Minimum backoff duration.
    - `{max, pos_integer()}` - Maximum backoff duration.
    - `{type, jitter | normal}`  - Type of backoff strategy.

## Usage Example
```
{Delay, Retry1} = partisan_retry:fail(Retry0),
Ref = partisan_retry:fire(Retry1).
```

## Return Semantics
Functions like `get/1` and `fail/1` return either the next delay (in ms) or
an atom indicating termination conditions: `max_retries` or `deadline`.

### Integration Notes
- Caller is responsible for invoking `fail/1` or `succeed/1` after outcome.
- The retry timer must be externally scheduled using `fire/1` and monitored.
""").

-record(partisan_retry, {
    id                  ::  any(),
    deadline            ::  non_neg_integer(),
    max_retries = 0     ::  non_neg_integer(),
    interval            ::  pos_integer(),
    count = 0           ::  non_neg_integer(),
    backoff             ::  optional(backoff:backoff()),
    start_ts            ::  optional(pos_integer())
}).

-type t()               ::  #partisan_retry{}.
-type opts()            ::  #{
                                deadline => non_neg_integer(),
                                max_retries => non_neg_integer(),
                                interval => pos_integer(),
                                backoff => backoff_opts()
                            }.
-type backoff_opts()      ::  #{
                                enabled => boolean(),
                                min => pos_integer(),
                                max => pos_integer(),
                                type => jitter | normal
                            }.

-export_type([t/0]).
-export_type([opts/0]).

-export([init/2]).
-export([fail/1]).
-export([succeed/1]).
-export([get/1]).
-export([fire/1]).
-export([count/1]).

-compile({no_auto_import, [get/1]}).

-eqwalizer({nowarn_function, init/2}).


%% =============================================================================
%% API
%% =============================================================================



?DOC("""
Initializes a new retry state with the given ID and options.

The retry mechanism can be based on a fixed interval or use exponential
backoff depending on options.

Set `deadline` to 0 to disable deadline tracking and rely solely on `max_retries`.
""").
-spec init(Id :: any(), Opts :: opts()) -> t().

init(Id, Opts) ->
    State0 = #partisan_retry{
        id = Id,
        max_retries = maps:get(max_retries, Opts, 10),
        deadline = maps:get(deadline, Opts, 30000),
        interval = maps:get(interval, Opts, 3000)
    },
    case maps:get(backoff, Opts, undefined) of
        undefined ->
            State0;

        BackoffOpts ->
            case maps:get(enabled, BackoffOpts, false) of
                true ->
                    Min = maps:get(min, Opts, 10),
                    Max = maps:get(max, Opts, 120000),
                    Type = maps:get(type, Opts, jitter),
                    Backoff = backoff:type(backoff:init(Min, Max), Type),
                    State0#partisan_retry{backoff = Backoff};

                false ->
                    State0
            end
    end.


?DOC("""
Returns the delay (in milliseconds) before the next retry should occur.

If no retries remain (`max_retries`) or the deadline has passed, returns
`max_retries` or `deadline` respectively.

Returns `integer()` if retry is allowed, or `deadline | max_retries` atom.
""").
-spec get(State :: t()) -> integer() | deadline | max_retries.

get(#partisan_retry{start_ts = undefined, backoff = undefined} = State) ->
    State#partisan_retry.interval;

get(#partisan_retry{start_ts = undefined, backoff = B}) ->
    backoff:get(B);

get(#partisan_retry{count = N, max_retries = M}) when N > M ->
    max_retries;

get(#partisan_retry{} = State) ->
    Now = erlang:system_time(millisecond),
    Deadline = State#partisan_retry.deadline,
    B = State#partisan_retry.backoff,

    Start =
        case State#partisan_retry.start_ts of
            undefined ->
                0;
            Val ->
                Val
        end,

    case Deadline > 0 andalso Now > (Start + Deadline) of
        true ->
            deadline;
        false when B == undefined ->
            State#partisan_retry.interval;
        false ->
            backoff:get(B)
    end.


?DOC("""
Increments the retry counter and computes the next delay.

If `max_retries` is reached, returns `{max_retries, State}`.
If a deadline is exceeded, `get/1` will later return `deadline`.

Automatically initializes the start timestamp on first retry.

Returns the tuple `{Delay, NewState}`, or `{max_retries | deadline, NewState}`.
""").
-spec fail(State :: t()) ->
    {Time :: integer(), NewState :: t()}
    | {deadline | max_retries, NewState :: t()}.

fail(#partisan_retry{max_retries = N, count = N} = State) ->
    {max_retries, State};

fail(#partisan_retry{backoff = undefined} = State0) ->
    State1 = State0#partisan_retry{
        count = State0#partisan_retry.count + 1
    },
    State = maybe_init_ts(State1),
    %% eqwalizer:ignore
    {get(State), State};

fail(#partisan_retry{backoff = B0} = State0) ->
    {_, B1} = backoff:fail(B0),

    State1 = State0#partisan_retry{
        count = State0#partisan_retry.count + 1,
        backoff = B1
    },
    State = maybe_init_ts(State1),
    %% eqwalizer:ignore
    {get(State), State}.


?DOC("""
Resets the retry counter and (if applicable) resets the backoff state.

Used after a successful attempt to stop retrying.

Returns the tuple of `{Delay, NewState}` where delay is the base interval or backoff.
""").
-spec succeed(State :: t()) -> {Time :: integer(), NewState :: t()}.

succeed(#partisan_retry{backoff = undefined} = State0) ->
    State = State0#partisan_retry{
        count = 0,
        start_ts = undefined
    },
    %% eqwalizer:ignore
    {get(State), State};

succeed(#partisan_retry{backoff = B0} = State0) ->
    {_, B1} = backoff:succeed(B0),
    State = State0#partisan_retry{
        count = 0,
        start_ts = undefined,
        backoff = B1
    },
    %% eqwalizer:ignore
    {get(State), State}.


?DOC("""
Starts a timer based on the current retry delay.

If the retry has expired (due to max retries or deadline), this function raises an error.

Returns the timer reference if successful; otherwise, throws `max_retries` or `deadline` error.
""").
-spec fire(State :: t()) -> Ref :: reference() | no_return().

fire(#partisan_retry{} = State) ->
    case get(State) of
        Delay when is_integer(Delay) ->
            erlang:start_timer(Delay, self(), State#partisan_retry.id);
        Other ->
            error(Other)
    end.


?DOC("""
Returns the number of failed retry attempts so far.
""").
-spec count(State :: t()) -> non_neg_integer().

count(#partisan_retry{count = Val}) ->
    Val.


%% =============================================================================
%% PRIVATE
%% =============================================================================



maybe_init_ts(#partisan_retry{start_ts = undefined} = State) ->
    State#partisan_retry{
        start_ts = erlang:system_time(millisecond)
    };

maybe_init_ts(#partisan_retry{} = State) ->
    State.
