%%%-------------------------------------------------------------------
%%% @author tihanyipeter
%%% @copyright (C) 2020, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(paxos).
-author("tihanyipeter").

-define(DEFAULT_ACCEPTOR_MODULE, paxos_ets_acceptor).

%% API
-export([ get/1, get/2,
          put/2,
          start_acceptor/0, start_acceptor/1,
          stop_acceptor/1]).

-spec get(Id :: term()) ->
  {ok ,term()} | {error, not_enough_acceptors} | not_found.
get(Id) ->
  paxos_proposer:get(Id).

-spec get(Id :: term(), NumberOfProposers :: pos_integer()) ->
  {ok ,term()} | {error, not_enough_acceptors} | not_found.
get(Id, Num) ->
  paxos_proposer:get(Id, Num).

-spec put(term(), term()) ->
  {already_agreed, term()} | accepted | {error, not_enough_acceptors} | timeout.
put(Key, Value) ->
  paxos_proposer:put(Key, Value).

-spec start_acceptor() -> pid().
start_acceptor() ->
  start_acceptor(?DEFAULT_ACCEPTOR_MODULE).

-spec start_acceptor(module()) -> pid().
start_acceptor(Module) ->
  paxos_acceptor_manager:start_acceptor(Module).

-spec stop_acceptor(pid()) ->
  ok | term().
stop_acceptor(Pid) ->
  paxos_acceptor_manager:stop_acceptor(Pid).
