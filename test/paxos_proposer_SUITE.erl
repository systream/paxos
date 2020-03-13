%%%-------------------------------------------------------------------
%%% @author tihanyipeter
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(paxos_proposer_SUITE).
-author("tihanyipeter").
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%% Info = [tuple()]
%%--------------------------------------------------------------------
suite() ->
  [{timetrap, {minutes, 5}}].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
init_per_suite(Config) ->
  net_kernel:set_net_ticktime(5),
  application:ensure_all_started(paxos),
  % stop all acceptors
  paxos_test_utils:stop_acceptors(),
  Config.

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> term() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
  ok.

%%--------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
init_per_group(GroupName, Config) ->
  apply(?MODULE, GroupName, [{prelude, Config}]).

%%--------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%               term() | {save_config,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%%--------------------------------------------------------------------
end_per_group(GroupName, Config) ->
  apply(?MODULE, GroupName, [{postlude, Config}]).

%%--------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
  paxos_test_utils:stop_acceptors(),
  Config.

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               term() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
  meck:unload(),
  ok.

%%--------------------------------------------------------------------
%% Function: groups() -> [Group]
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%% Shuffle = shuffle | {shuffle,{integer(),integer(),integer()}}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%% N = integer() | forever
%%--------------------------------------------------------------------
groups() -> [].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = term()
%%--------------------------------------------------------------------
all() ->
  [
    get_not_enough_acceptors,
    get_not_set,
    get_not_enough_num,
    get_setted,
    get_timeout,
    put_not_enough_acceptors,
    put_timeout,
    put_prepare_already_agreed,
    put_accept_already_agreed,
    put_prepare_already_agreed_same_value,
    put_accept_already_agreed_same_value,

    put_prepare_majority_rejected_value,
    put_prepare_minority_rejected_value,
    put_accept_majority_rejected_value,
    put_accept_minority_rejected_value
  ].

%%--------------------------------------------------------------------
%% Function: TestCase() -> Info
%% Info = [tuple()]
%%--------------------------------------------------------------------


%%--------------------------------------------------------------------
%% Function: TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%%--------------------------------------------------------------------

get_not_enough_acceptors(_Config) ->
  ?assertEqual({error, not_enough_acceptors}, paxos:get(test)),
  ?assertEqual({error, not_enough_acceptors}, paxos:get(test, 1)).

get_not_set(_Config) ->
  paxos:start_acceptor(),
  ?assertEqual(not_found, paxos:get(test)).

get_not_enough_num(_Config) ->
  paxos:start_acceptor(),
  {_, Pid} = paxos_acceptor_manager:get_acceptors(),
  ?assertEqual({error, not_enough_acceptors}, paxos:get(test, length(Pid)+1)).

get_setted(_Config) ->
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  ?assertEqual(accepted, paxos:put(test, self())),
  ?assertEqual({ok, self()}, paxos:get(test)),
  ?assertEqual({ok, self()}, paxos:get(test, 1)).

get_timeout(_Config) ->
  ok = meck:new(paxos_acceptor, [passthrough]),
  meck:expect(paxos_acceptor,
    send_get_request,
    fun(_Pid, _Key) ->
      ok
    end),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  ?assertEqual(ignore, paxos:get(test1)),
  ?assertEqual(ignore, paxos:get(test1, 2)),
  meck:unload(paxos_acceptor).

put_not_enough_acceptors(_Config) ->
  ?assertEqual({error, not_enough_acceptors}, paxos:put(test, foo)).

put_timeout(_Config) ->
  ok = meck:new(paxos_acceptor, [passthrough]),
  meck:expect(paxos_acceptor,
              accept_request,
              fun(_Pid, _Id, _Key, _Value) ->
                ok
              end),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  ?assertEqual(timeout, paxos:put(test1, self())),
  paxos_test_utils:stop_acceptors(),

  paxos:start_acceptor(),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  meck:expect(paxos_acceptor,
    prepare_request,
    fun(_Pid, _Id, _Key) ->
      ok
    end),
  ?assertEqual(timeout, paxos:put(test1, self())),
  meck:unload(paxos_acceptor).


put_prepare_already_agreed(_Config) ->
  ok = meck:new(paxos_acceptor, [passthrough]),
  meck:expect(paxos_acceptor,
    prepare_request,
    fun(_Pid, Id, Key) ->
      self() ! {already_agreed, Id, Key, test},
      ok
    end),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  ?assertEqual({already_agreed, test}, paxos:put(test3, self())),
  meck:unload(paxos_acceptor).

put_accept_already_agreed(_Config) ->
  ok = meck:new(paxos_acceptor, [passthrough]),
  meck:expect(paxos_acceptor,
    accept_request,
    fun(_Pid, Id, Key, _Value) ->
      self() ! {already_agreed, Id, Key, test},
      ok
    end),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  Result = paxos:put(test4, self()),
  ?assertEqual({already_agreed, test}, Result),
  meck:unload(paxos_acceptor).

put_prepare_already_agreed_same_value(_Config) ->
  ok = meck:new(paxos_acceptor, [passthrough]),
  meck:expect(paxos_acceptor,
    prepare_request,
    fun(_Pid, Id, Key) ->
      self() ! {already_agreed, Id, Key, foo},
      ok
    end),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  ?assertEqual(accepted, paxos:put(test5, foo)),
  meck:unload(paxos_acceptor).

put_accept_already_agreed_same_value(_Config) ->
  ok = meck:new(paxos_acceptor, [passthrough]),
  meck:expect(paxos_acceptor,
    accept_request,
    fun(_Pid, Id, Key, _Value) ->
      self() ! {already_agreed, Id, Key, foo},
      ok
    end),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  paxos:start_acceptor(),
  ?assertEqual(accepted, paxos:put(test6, foo)),
  meck:unload(paxos_acceptor).


put_prepare_majority_rejected_value(_Config) ->
  ok = meck:new(paxos_acceptor, [passthrough]),
  Pid1 = paxos:start_acceptor(),
  Pid2 = paxos:start_acceptor(),
  _Pid3 = paxos:start_acceptor(),

  meck:expect(paxos_acceptor,
    prepare_request,
    fun (Pid, Id, Key) when Pid == Pid1 ->
          meck:passthrough([Pid, Id, Key]);
        (_Pid, Id, Key) when Id < 100 ->
          self() ! {reject, Id, Key, Id+1};
        (Pid, Id, Key) ->
          meck:passthrough([Pid, Id, Key])
    end),
  ?assertEqual(accepted, paxos:put(test6, foo)),
  NumCalls = meck:num_calls(paxos_acceptor, prepare_request, [Pid2, '_', '_']),
  ?assertEqual(true, (100 div 4) =< NumCalls),
  meck:unload(paxos_acceptor).

put_prepare_minority_rejected_value(_Config) ->
  ok = meck:new(paxos_acceptor, [passthrough]),
  Pid1 = paxos:start_acceptor(),
  _Pid2 = paxos:start_acceptor(),
  _Pid3 = paxos:start_acceptor(),

  meck:expect(paxos_acceptor,
    prepare_request,
    fun (Pid, Id, Key) when Pid =/= Pid1 ->
      meck:passthrough([Pid, Id, Key]);
      (_Pid, Id, Key) ->
        self() ! {reject, Id, Key, 100}
    end),
  ?assertEqual(accepted, paxos:put(test6, foo)),
  meck:unload(paxos_acceptor).



put_accept_majority_rejected_value(_Config) ->
  ok = meck:new(paxos_acceptor, [passthrough]),
  Pid1 = paxos:start_acceptor(),
  Pid2 = paxos:start_acceptor(),
  _Pid3 = paxos:start_acceptor(),

  meck:expect(paxos_acceptor,
    accept_request,
    fun (Pid, Id, Key, Value) when Pid == Pid1 ->
      meck:passthrough([Pid, Id, Key, Value]);
      (_Pid, Id, Key, _Value) when Id < 100 ->
        self() ! {reject, Id, Key, Id+1};
      (Pid, Id, Key, Value) ->
        meck:passthrough([Pid, Id, Key, Value])
    end),
  ?assertEqual(accepted, paxos:put(test6, foo)),
  NumCalls = meck:num_calls(paxos_acceptor, accept_request, [Pid2, '_', '_', '_']),
  ?assertEqual(true, (100 div 4) =< NumCalls),
  meck:unload(paxos_acceptor).

put_accept_minority_rejected_value(_Config) ->
  ok = meck:new(paxos_acceptor, [passthrough]),
  Pid1 = paxos:start_acceptor(),
  _Pid2 = paxos:start_acceptor(),
  _Pid3 = paxos:start_acceptor(),

  meck:expect(paxos_acceptor,
    accept_request,
    fun (Pid, Id, Key, Value) when Pid =/= Pid1 ->
      meck:passthrough([Pid, Id, Key, Value]);
      (_Pid, Id, Key, _Value) ->
        self() ! {reject, Id, Key, 100}
    end),
  ?assertEqual(accepted, paxos:put(test6, foo)),
  meck:unload(paxos_acceptor).