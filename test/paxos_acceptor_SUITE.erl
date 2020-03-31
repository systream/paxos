%%%-------------------------------------------------------------------
%%% @author tihanyipeter
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Mar 2020 20:37
%%%-------------------------------------------------------------------
-module(paxos_acceptor_SUITE).
-author("tihanyipeter").
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%% Info = [tuple()]
%%--------------------------------------------------------------------
suite() ->
  [{timetrap, {minutes, 2}}].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
init_per_suite(Config) ->
  application:ensure_all_started(paxos),
  % stop all acceptors
  paxos_test_utils:stop_acceptors(),
  Config.

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> term() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
  application:stop(paxos),
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
groups() ->
  [
    {ets_group, [{repeat, 1}, parallel], [
      prepare_ok,
      prepare_same_id,
      prepare_reject_lower_id,
      prepare_accept_higher_value,
      prepare_already_agreed_on_value,
      accept_ok,
      accept_rejected_different_id,
      accept_not_prepared,
      accept_already_agree,
      unknown_msg,
      cannot_start_process,
      get_not_set,
      get_prepare,
      get_accept,
      get_timeout,
      sync_not_set,
      sync_prepare,
      sync_accept,
      fold_empty,
      fold
    ]},
    {dets_group, [{repeat, 1}, parallel], [
      prepare_ok,
      prepare_same_id,
      prepare_reject_lower_id,
      prepare_accept_higher_value,
      prepare_already_agreed_on_value,
      accept_ok,
      accept_rejected_different_id,
      accept_not_prepared,
      accept_already_agree,
      unknown_msg,
      cannot_start_process,
      get_not_set,
      get_prepare,
      get_accept,
      get_timeout,
      sync_not_set,
      sync_prepare,
      sync_accept,
      fold_empty,
      fold
    ]}
  ].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = term()
%%--------------------------------------------------------------------
all() ->
  [
    {group, ets_group},
    {group, dets_group}
  ].

%%--------------------------------------------------------------------
%% Function: TestCase() -> Info
%% Info = [tuple()]
%%--------------------------------------------------------------------

ets_group({prelude, Config}) ->
  [{pid, init_group(paxos_ets_acceptor)} | Config];
ets_group({postlude, Config}) ->
  exit(proplists:get_value(pid, Config), kill),
  ok.

dets_group({prelude, Config}) ->
  [{pid, init_group(paxos_dets_acceptor)} | Config];
dets_group({postlude, Config}) ->
  exit(proplists:get_value(pid, Config), kill),
  ok.


init_group(Module) ->
  Self = self(),
  spawn(fun() ->
          {ok, Pid} = paxos_acceptor:start_link(Module, []),
          Self ! {pid, Pid}
        end),
  {ok, Pid} = receive
                {pid, PPid} ->
                  {ok, PPid}
              after 1000 ->
                {error, timeout}
              end,
  Pid.
%%--------------------------------------------------------------------
%% Function: TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%%--------------------------------------------------------------------
prepare_ok(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test",
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  Result = receive_response(1, Key),
  flush(),
  ?assertEqual(promise_ok, Result).

prepare_same_id(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test1",
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  receive_response(1, Key),
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  Result = receive_response(1, Key),
  flush(),
  ?assertEqual({reject, 1}, Result).

prepare_reject_lower_id(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_lower_id",
  ok = paxos_acceptor:prepare_request(Pid, 2, Key),
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  Result = receive_response(1, Key),
  flush(),
  ?assertEqual({reject, 2}, Result).

prepare_accept_higher_value(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_higher_id",
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  ok = paxos_acceptor:prepare_request(Pid, 2, Key),
  Result = receive_response(2, Key),
  flush(),
  ?assertEqual(promise_ok, Result).

prepare_already_agreed_on_value(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_already_agreed",
  ok = paxos_acceptor:prepare_request(Pid, 10, Key),
  promise_ok = receive_response(10, Key),
  paxos_acceptor:accept_request(Pid, 10, Key, 456),
  accepted_ok = receive_response(10, Key),
  ok = paxos_acceptor:prepare_request(Pid, 12, Key),
  Result = receive_response(12, Key),
  flush(),
  ?assertEqual({already_agreed, 456}, Result).

accept_ok(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_2",
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  promise_ok = receive_response(1, Key),
  paxos_acceptor:accept_request(Pid, 1, Key, 456),
  Result = receive_response(1, Key),
  flush(),
  ?assertEqual(accepted_ok, Result).

accept_rejected_different_id(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_3",
  ok = paxos_acceptor:prepare_request(Pid, 10, Key),
  promise_ok = receive_response(10, Key),
  paxos_acceptor:accept_request(Pid, 1, Key, 456),
  Result = receive_response(1, Key),
  flush(),
  ?assertEqual({reject, 10}, Result),

  paxos_acceptor:accept_request(Pid, 11, Key, 456),
  Result = receive_response(11, Key),
  flush(),
  ?assertEqual({reject, 10}, Result).

accept_not_prepared(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_4",
  paxos_acceptor:accept_request(Pid, 1, Key, 456),
  Result = receive_response(1, Key),
  flush(),
  ?assertEqual({reject, 1}, Result).

accept_already_agree(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_5",
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  promise_ok = receive_response(1, Key),
  paxos_acceptor:accept_request(Pid, 1, Key, 456),
  accepted_ok = receive_response(1, Key),
  paxos_acceptor:accept_request(Pid, 10, Key, 443),
  Result = receive_response(10, Key),
  flush(),
  ?assertEqual({already_agreed, 456}, Result).

unknown_msg(Config) ->
  Pid = proplists:get_value(pid, Config),
  Pid ! test_unkonw_msg,
  timer:sleep(100),
  ?assertEqual({message_queue_len, 0}, erlang:process_info(self(), message_queue_len)).

cannot_start_process(_Config) ->
  ?assertEqual({error, timeout}, paxos_acceptor:start_link(paxos_acceptor_fixture, [])).


get_not_set(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_get_test",
  ?assertEqual(not_found, paxos_acceptor:get(Pid, Key, 100)).


get_prepare(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_get_test2",
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  promise_ok = receive_response(1, Key),
  ?assertEqual(not_found, paxos_acceptor:get(Pid, Key, 100)).

get_accept(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_get_test4",
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  promise_ok = receive_response(1, Key),
  paxos_acceptor:accept_request(Pid, 1, Key, 456),
  accepted_ok = receive_response(1, Key),
  ?assertEqual({ok, 456}, paxos_acceptor:get(Pid, Key, 100)).

get_timeout(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_get_test5",
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  promise_ok = receive_response(1, Key),
  paxos_acceptor:accept_request(Pid, 1, Key, 456),
  accepted_ok = receive_response(1, Key),
  ?assertEqual(timeout, paxos_acceptor:get(Pid, Key, 0)).


sync_not_set(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_sync_test",
  ok = paxos_acceptor:sync(Pid, Key, foo),
  ?assertEqual({ok, foo}, paxos_acceptor:get(Pid, Key, 100)),
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  {already_agreed, foo} = receive_response(1, Key).

sync_prepare(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_sync_test2",
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  promise_ok = receive_response(1, Key),
  ok = paxos_acceptor:sync(Pid, Key, foo),
  ?assertEqual({ok, foo}, paxos_acceptor:get(Pid, Key, infinity)),
  ok = paxos_acceptor:accept_request(Pid, 1, Key, foo3),
  {already_agreed, foo} = receive_response(1, Key).

sync_accept(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_sync_test4",
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  promise_ok = receive_response(1, Key),
  paxos_acceptor:accept_request(Pid, 1, Key, 456),
  accepted_ok = receive_response(1, Key),
  ok = paxos_acceptor:sync(Pid, Key, foo),
  ?assertEqual({ok, foo}, paxos_acceptor:get(Pid, Key, 100)).

fold_empty(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_sync_test14",
  ok = paxos_acceptor:prepare_request(Pid, 1, Key),
  Self = self(),
  paxos_acceptor:fold(Pid, fun(FKey, Value) -> Self ! {kv, FKey, Value} end),
  Result =
    receive
      {kv, KeyR, _Value} when KeyR == Key->
        found
      after 100 ->
      not_found
    end,
  ?assertEqual(not_found, Result).

fold(Config) ->
  Pid = proplists:get_value(pid, Config),
  Key = "test_sync_test21",
  ok = paxos_acceptor:sync(Pid, Key, foo),
  Self = self(),
  paxos_acceptor:fold(Pid, fun(FKey, Value) -> Self ! {kv, FKey, Value} end),
  Result =
    receive
      {kv, KeyR, foo} when KeyR == Key ->
        found
    after 100 ->
      not_found
    end,
  ?assertEqual(found, Result).

flush() ->
  receive
    A ->
      ct:pal("flush: ~p~n", [A]),
      flush()
  after 0 ->
    ct:pal("nothing to flush")
  end.

receive_response(Id, Key) ->
  receive
    {promise, Id, Key} ->
      promise_ok;
    {accepted, Id, Key} ->
      accepted_ok;
    {reject, Id, Key, NewId} ->
      {reject, NewId};
    {already_agreed, Id, Key, Value} ->
      {already_agreed, Value}
  after 1000 ->
    timeout
  end.