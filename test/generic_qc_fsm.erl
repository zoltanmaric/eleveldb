%% -------------------------------------------------------------------
%%
%% Testing testing testing
%%
%% Copyright (c) 2014 Basho Technologies, Inc. All Rights Reserved.
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
-module(generic_qc_fsm).
%% Borrowed heavily from bitcask_qc_fsm.erl

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{ handle,
                data = [],
                keys }). %% Keys to use in the test

%% Used for output within EUnit...
-define(QC_FMT(Fmt, Args),
        io:format(user, Fmt, Args)).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(TEST_DIR, "/tmp/generic.qc").

coredump1(FI_enabledP) ->
    faulterl_nif:poke("bc_fi_enabled", 0, <<0:8/native>>, false),
    ok = really_delete_dir(?TEST_DIR),

    if FI_enabledP ->
            faulterl_nif:poke("bc_fi_enabled", 0, <<1:8/native>>, false),

            faulterl_nif:poke("bc_fi_random_seed", 0, <<43:32/native>>, false),
            faulterl_nif:poke("bc_fi_random_reseed", 0, <<1:8/native>>, false);
       true ->
            ok
    end,

    H1 = open(?TEST_DIR, [read_write,
                          {open_timeout, 0},
                          {max_file_size, 100}]),
    delete(H1, <<"k">>),
    put(H1, <<"k">>, <<>>),
    close(H1),
    _H2 = open(?TEST_DIR),
    we_all_win.

initial_state() ->
    init.

initial_state_data() ->
    #state{}.

init(_S) ->
    [{closed, {call, ?MODULE, set_keys, [list(key_gen())]}}].

closed(_S) ->
    [{opened, {call, ?MODULE, open, [?TEST_DIR, [read_write,
                                                 {open_timeout, 0},
                                                 {max_file_size, 100},
                                                 sync_strategy()]]}}
    ].

opened(S) ->
    [{closed, {call, ?MODULE, close, [S#state.handle]}},
     {opened, {call, ?MODULE, get, [S#state.handle, key(S)]}},
     {opened, {call, ?MODULE, put, [S#state.handle, key(S), value()]}},
     {opened, {call, ?MODULE, delete, [S#state.handle, key(S)]}},
     {opened, {call, ?MODULE, merge, [?TEST_DIR]}}
     ].

next_state_data(init, closed, S, _, {call, _, set_keys, [Keys]}) ->
    S#state{ keys = [<<"k">> | Keys] }; % ensure always one key
next_state_data(closed, opened, S, Handle, {call, _, open, _}) ->
    S#state { handle = Handle };
next_state_data(opened, closed, S, _, {call, _, close, _}) ->
    S#state { handle = undefined };
next_state_data(opened, opened, S, _, {call, _, put, [_, Key, Value]}) ->
    S#state { data = orddict:store(Key, Value, S#state.data) };
next_state_data(opened, opened, S, _, {call, _, delete, [_, Key]}) ->
    S#state { data = orddict:erase(Key, S#state.data) };
next_state_data(_From, _To, S, _Res, _Call) ->
    S.




%% Precondition (for state data).
%% Precondition is checked before command is added to the command sequence
precondition(_From,_To,S,{call,_,get,[_,Key]}) ->
    lists:member(Key, S#state.keys); % check the key has not been shrunk away
precondition(_From,_To,S,{call,_,put,[_,Key,_Val]}) ->
    lists:member(Key, S#state.keys); % check the key has not been shrunk away
precondition(_From,_To,_S,{call,_,_,_}) ->
    true.


postcondition(_OldSt, _NewSt, _S, {call, _, _Func, _Args}, _Res) ->
    true.
%% postcondition(opened, opened, S, {call, _, get, [_, Key]}, not_found) ->
%%     case orddict:find(Key, S#state.data) of
%%         error ->
%%             true;
%%         {ok, Exp} ->
%%             {expected, Exp, got, not_found}
%%     end;
%% postcondition(opened, opened, S, {call, _, get, [_, Key]}, {ok, Value}) ->
%%     case orddict:find(Key, S#state.data) of
%%         {ok, Value} ->
%%             true;
%%         Exp ->
%%             {expected, Exp, got, Value}
%%     end;
%% postcondition(opened, opened, _S, {call, _, merge, [_TestDir]}, Res) ->
%%     Res == ok;
%% postcondition(_From,_To,_S,{call,_,_,_},_Res) ->
%%     true.

qc_test_() ->
    TestTime = 45,
    {timeout, TestTime*2,
     {setup, fun prepare/0, fun cleanup/1,
      %% Run for one second without FI to allow code loader to load everything
      %% without interference from artificial faults.
      [{timeout, TestTime*2, ?_assertEqual(true,
                eqc:quickcheck(eqc:testing_time(1, ?QC_OUT(prop(false)))))},
       %% TODO: check an OS env var or check result of an experimental peek
       %%       to see if we can run with FI, then change the arg below!
       {timeout, TestTime*2, ?_assertEqual(true,
                eqc:quickcheck(eqc:testing_time(TestTime, ?QC_OUT(prop()))))}
      ]}}.

prepare() ->
    ok.

cleanup(_) ->
    ok.

prop() ->
    prop(false).

prop(FI_enabledP) ->
    prop(FI_enabledP, false).

prop(FI_enabledP, VerboseP) ->
    _ = faulterl_nif:poke("bc_fi_enabled", 0, <<0:8/native>>, false),
    ?FORALL({Cmds, Seed}, {commands(?MODULE), choose(1,9999)},
            begin
                faulterl_nif:poke("bc_fi_enabled", 0, <<0:8/native>>, false),
                ok = really_delete_dir(?TEST_DIR),
                event_logger:start_link(),
                if FI_enabledP ->
                        ok = faulterl_nif:poke("bc_fi_enabled", 0,
                                               <<1:8/native>>, false),
                        VerboseI = if VerboseP -> 1;
                                      true     -> 0 end,
                        ok = faulterl_nif:poke("bc_fi_verbose", 0,
                                               <<VerboseI:8/native>>, false),

                        ok = faulterl_nif:poke("bc_fi_random_seed", 0,
                                               <<Seed:32/native>>, false),
                        ok = faulterl_nif:poke("bc_fi_random_reseed", 0,
                                               <<1:8/native>>, false);
                   true ->
                        ok
                end,
                event_logger:start_logging(),
                {H,{_State, StateData}, Res} = run_commands(?MODULE,Cmds),
                _ = faulterl_nif:poke("bc_fi_enabled", 0, <<0:8/native>>, false),
                case (StateData#state.handle) of
                    undefined ->
                        ok;
                    Handle ->
                        close(Handle)
                end,
                %% application:unload(bitcask),
                Trace0 = event_logger:get_events(),
                Trace = remove_timestamps(Trace0),
                Sane = verify_trace(Trace),

                ?WHENFAIL(
                ?QC_FMT("Trace: ~p\nverify_trace: ~p\n", [Trace, Sane]),
                aggregate(zip(state_names(H),command_names(Cmds)), 
                          conjunction([{postconditions, equals(Res, ok)},
                                       {verify_trace, Sane}])))
            end).

remove_timestamps(Trace) ->
    [Event || {_TS, Event} <- Trace].

verify_trace([]) ->
    true;
verify_trace([{set_keys, Keys0}|TraceTail]) ->
    Keys = [<<"k">>|Keys0],
    Dict0 = dict:from_list([{K, [not_found]} || K <- Keys]),
    {Bool, _D} =
        lists:foldl(
          fun({get, K, V}, {true, D}) ->
                  Vs = dict:fetch(K, D),
                  case lists:member(V, Vs) of
                      true ->
                          {true, D};
                      false ->
                          {{get,expected,Vs,got,V}, D}
                  end;
             ({put, yes, K, V}, {true, D}) ->
                  {true, dict:store(K, [V], D)};
             ({put, maybe, K, V, _Err}, {true, D}) ->
                  io:format(user, "pm,", []),
                  Vs = dict:fetch(K, D),
                  {true, dict:store(K, [V|Vs], D)};
             ({delete, yes, K}, {true, D}) ->
                  {true, dict:store(K, [not_found], D)};
             ({delete, maybe, K, _Err}, {true, D}) ->
                  io:format(user, "dm,", []),
                  Vs = dict:fetch(K, D),
                  {true, dict:store(K, [not_found|Vs], D)};
             (_, Acc) ->
                  Acc
          end, {true, Dict0}, TraceTail),
    Bool.

%% Weight for transition (this callback is optional).
%% Specify how often each transition should be chosen
weight(_From, _To,{call,_,close,_}) ->
    10;
weight(_From, _To,{call,_,merge,_}) ->
    5;
weight(_From,_To,{call,_,_,_}) ->
    100.

set_keys(Keys) -> %% next_state sets the keys for use by key()
    event_logger:event({set_keys, Keys}),
    ok.

key_gen() ->
    ?SUCHTHAT(X, binary(), X /= <<>>).

key(#state{keys = Keys}) ->
    elements(Keys).

value() ->
    binary().

sync_strategy() ->
    {sync_strategy, oneof([none])}.

really_delete_dir(Dir) ->
    [file:delete(X) || X <- filelib:wildcard(Dir ++ "/*")],
    [file:delete(X) || X <- filelib:wildcard(Dir ++ "/*/*")],
    [file:del_dir(X) || X <- filelib:wildcard(Dir ++ "/*")],
    case file:del_dir(Dir) of
        ok             -> ok;
        {error,enoent} -> ok;
        Else           -> Else
    end.

%% open(Dir, Opts) ->
%%     %% io:format(user, "open,", []),
%%     bitcask:open(Dir, Opts).

%% close(H) ->
%%     %% io:format(user, "close,", []),
%%     bitcask:close(H).

%% get(H, K) ->
%%     %% io:format(user, "get ~p,", [K]),
%%     bitcask:get(H, K).

%% put(H, K, V) ->
%%     %% io:format(user, "put ~p,", [K]),
%%     bitcask:put(H, K, V).

%% delete(H, K) ->
%%     %% io:format(user, "delete ~p,", [K]),
%%     bitcask:delete(H, K).

%% merge(H) ->
%%     bitcask:merge(H).

open(Dir, Opts) ->
    case eleveldb:open(Dir, [{create_if_missing,true},
                             {limited_developer_mem, true}] ++ Opts) of
        {ok, H} ->
            event_logger:event(open),
            H;
        X ->
            event_logger:event({open, X}),
            not_open
    end.

close(not_open) ->
    ok;
close(H) ->
    case eleveldb:close(H) of
        ok = X ->
            event_logger:event(close),
            X;
        X ->
            event_logger:event({close, X}),
            X
    end.

get(not_open, _K) ->
    get_result_ignored;
get(H, K) ->
    case eleveldb:get(H, K, []) of
        {ok, V} = X ->
            event_logger:event({get, K, V}),
            X;
        not_found = X ->
            event_logger:event({get, K, not_found}),
            X
    end.

put(not_open, _K, _v) ->
    put_result_ignored;
put(H, K, V) ->
    case eleveldb:put(H, K, V, []) of
        ok = X ->
            event_logger:event({put, yes, K, V}),
            X;
        X ->
            %io:format("PUT -> ~p, ", [X]),
            event_logger:event({put, maybe, K, V, X}),
            X
    end.

delete(not_open, _K) ->
    delete_result_ignored;
delete(H, K) ->
    case eleveldb:delete(H, K, []) of
        ok = X ->
            event_logger:event({delete, yes, K}),
            X;
        X ->
            event_logger:event({delete, maybe, K, X}),
            X
    end.

merge(_H) ->
    ok. % Noop for eleveldb

-endif.


