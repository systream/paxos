%%%-------------------------------------------------------------------
%%% @author tihanyipeter
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(paxos_acceptor_manager_SUITE).
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
    empty_start,
    majority_check,
    join_node,
    sync_between_nodes,
    handle_node_down,
    remove_down_acceptor,
    sync_missing_node_info
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
empty_start(_Config) ->
  ?assertEqual({1, []}, paxos_acceptor_manager:get_acceptors()).

majority_check(_Config) ->
  Pid = paxos:start_acceptor(paxos_ets_acceptor),
  List1 = [Pid],
  ?assertEqual({1, List1}, paxos_acceptor_manager:get_acceptors()),

  Pid1 = paxos_acceptor_manager:start_acceptor(paxos_dets_acceptor),
  List2 = [Pid1 | List1],
  ?assertEqual({2, List2}, paxos_acceptor_manager:get_acceptors()),

  Pid2 = paxos:start_acceptor(),
  List3 = [Pid2 | List2],
  ?assertEqual({2, List3}, paxos_acceptor_manager:get_acceptors()),
  [paxos:start_acceptor() || _ <- lists:seq(1, 10)],
  {Majority, CurrentList} = paxos_acceptor_manager:get_acceptors(),

  ?assertEqual(7, Majority),
  ?assertEqual(13, length(CurrentList)).

join_node(_Config) ->
  HostNode = paxos_test_utils:start_slave_node('slave1'),
  RemoteAcceptorList =
    rpc:call(HostNode, paxos_acceptor_manager, get_acceptors, []),
  ?assertEqual(
    paxos_acceptor_manager:get_acceptors(),
    RemoteAcceptorList
  ).

sync_between_nodes(_Config) ->
  HostNode = paxos_test_utils:start_slave_node('slave2'),
  Pid = rpc:call(HostNode, paxos, start_acceptor, []),
  {_, ListOfLocalAcceptors} = paxos_acceptor_manager:get_acceptors(),
  ?assertEqual(true, lists:member(Pid, ListOfLocalAcceptors)),

  PidLocal = paxos:start_acceptor(),
  {_, ListOfAcceptors} = rpc:call(HostNode,  paxos_acceptor_manager, get_acceptors, []),
  ?assertEqual(true, lists:member(PidLocal, ListOfAcceptors)),

  % stop local acceptor
  paxos:stop_acceptor(PidLocal),
  {_, ListOfAcceptors2} = rpc:call(HostNode,  paxos_acceptor_manager, get_acceptors, []),
  ?assertEqual(false, lists:member(PidLocal, ListOfAcceptors2)),

  % stop non local acceptor
  paxos:stop_acceptor(Pid),
  {_, ListOfAcceptors3} = paxos_acceptor_manager:get_acceptors(),
  ?assertEqual(false, lists:member(PidLocal, ListOfAcceptors3)).


handle_node_down(_Config) ->
  HostNode = paxos_test_utils:start_slave_node('slave3'),
  Pid = rpc:call(HostNode, paxos, start_acceptor, []),
  {Majority, _} = paxos_acceptor_manager:get_acceptors(),
  ct_slave:stop(HostNode),
  {Majority2, Acceptors2} = paxos_acceptor_manager:get_acceptors(),
  ?assertEqual(Majority, Majority2),
  ?assertEqual(true, lists:member(Pid, Acceptors2)).

remove_down_acceptor(_Config) ->
  HostNode = paxos_test_utils:start_slave_node('slave4'),
  HostNode2 = paxos_test_utils:start_slave_node('slave5'),
  Pid = rpc:call(HostNode, paxos, start_acceptor, []),
  ct_slave:stop(HostNode),
  paxos:stop_acceptor(Pid),
  {_, Acceptors} = paxos_acceptor_manager:get_acceptors(),
  {_, Acceptors2} = rpc:call(HostNode2, paxos_acceptor_manager, get_acceptors, []),
  ?assertEqual(length(Acceptors), length(Acceptors2)),
  ?assertEqual(false, lists:member(Pid, Acceptors)).


sync_missing_node_info(_Config) ->
  DummyNode = 'dummy@test',
  ok = meck:new(paxos_acceptor_manager, [passthrough]),
  Self = self(),
  meck:expect(paxos_acceptor_manager, get_acceptors_by_node,
              fun(Node) ->
                [Self | meck:passthrough([Node])]
              end),

  HostNode = paxos_test_utils:start_slave_node('slave6'),

  AcceptorManagerPid = rpc:call(HostNode, erlang, whereis, [paxos_acceptor_manager]),
  AcceptorManagerPid ! {nodedown, DummyNode},
  ct:sleep(100),
  meck:unload(paxos_acceptor_manager),
  {_, Acceptors} = rpc:call(HostNode, paxos_acceptor_manager, get_acceptors, []),
  ?assertEqual(true, lists:member(Self, Acceptors)).
