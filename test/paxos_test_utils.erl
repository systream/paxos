%%%-------------------------------------------------------------------
%%% @author tihanyipeter
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Mar 2020 20:30
%%%-------------------------------------------------------------------
-module(paxos_test_utils).
-author("tihanyipeter").

%% API
-export([start_slave_node/1, stop_acceptors/0]).

start_slave_node(NodeName) ->
  ErlFlags = "-pa ../../../../_build/test/lib/*/ebin",
  {ok, HostNode} = ct_slave:start(NodeName,
    [ {kill_if_fail, true},
      {monitor_master, true},
      {init_timeout, 3000},
      {startup_timeout, 3000},
      {startup_functions,
        [{application, ensure_all_started, [paxos]}]},
      {erl_flags, ErlFlags}]),
  HostNode.


stop_acceptors() ->
  {_, Pids} = paxos_acceptor_manager:get_acceptors(),
  [paxos:stop_acceptor(Pid) || Pid <- Pids].