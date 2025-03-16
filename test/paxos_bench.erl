%%%-------------------------------------------------------------------
%%% @author tihi
%%% @copyright (C) 2025, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. márc. 2025 19:00
%%%-------------------------------------------------------------------
-module(paxos_bench).
-author("tihi").

%% API
-export([test/3]).


test(P, Keyspace, Item) ->
  wait([spawn_monitor(fun() -> work(Keyspace, Item) end) || _ <- lists:seq(1, P)]).

wait([{Pid, Ref} | Rem]) ->
  receive
    {'DOWN', Ref, process, Pid, _Reason} ->
      wait(Rem)
  end;
wait([]) ->
  ok.
work(_, 0) ->
  ok;
work(KeySpace, Item) ->
  paxos:put(10 + rand:uniform(KeySpace), erlang:unique_integer()),
  work(KeySpace, Item-1).
