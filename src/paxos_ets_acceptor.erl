%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(paxos_ets_acceptor).
-author("tihanyipeter").

-behavior(paxos_acceptor).

%-define(LOG(F, A), io:format(F, A)).
%-define(LOG(_F, _A), ok).
-define(LOG(F, A), ct:pal(F, A)).

%% API
-export([prepare/3, init/0, accept/4, sync/3, get/2]).

init() ->
  {ok, ets:new(paxos_acceptor, [ordered_set, public])}.

-spec prepare(Id :: pos_integer(), Ref :: term(), State) ->
  {accepted, State} |
  {rejected, ActualId :: pos_integer(), State} |
  {already_agreed, Value :: term(), State}
  when State :: term().
prepare(NewId, Ref, EtsRef) ->
  case ets:lookup(EtsRef, Ref) of
    [] ->
      true = ets:insert(EtsRef, {Ref, prepare, NewId}),
      ?LOG("[~p] Prepare request accepted. NewId ~p LastId: No prev id pid: ~p ~n", [Ref, NewId, self()]),
      {accepted, EtsRef};
    [{Ref, prepare, Id}] when NewId =< Id ->
      ?LOG("[~p] Prepare request rejected. NewId ~p LastId ~p pid: ~p ~n", [Ref, NewId, Id, self()]),
      {rejected, EtsRef, Id};
    [{Ref, prepare, _Id}] ->
      true = ets:insert(EtsRef, {Ref, prepare, NewId}),
      ?LOG("[~p] Prepare request accepted. NewId ~p LastId: ~p pid: ~p~n", [Ref, NewId, _Id, self()]),
      {accepted, EtsRef};
    [{Ref, accepted, Value}] ->
      ?LOG("[~p] Prepare request rejected. Already agreed. NewId ~p pid: ~p~n", [Ref, NewId, self()]),
      {already_agreed, Value, EtsRef}
  end.


-spec accept(Id :: pos_integer(), Ref :: term(), Value :: term(), State) ->
  {accepted, State} |
  {rejected, ActualId :: pos_integer(), State} |
  {already_agreed, Value :: term(), State}
  when State :: term().
accept(Id, Ref, Value, EtsRef) ->
  case ets:lookup(EtsRef, Ref) of
    [{Ref, prepare, Id}] ->
      ?LOG("[~p] Accept request accepted. Id ~p Value ~p pid: ~p~n", [Ref, Id, Value, self()]),
      true = ets:insert(EtsRef, {Ref, accepted, Value}),
      {accepted, EtsRef};
    [] ->
      ?LOG("[~p] Accept request rejected. No promise. Id ~p Value ~p pid: ~p~n", [Ref, Id, Value, self()]),
      {rejected, EtsRef, 1};
    [{Ref, prepare, NewId}] ->
      ?LOG("[~p] Accept request rejected. Id ~p NewId: ~p Value ~p pid: ~p~n", [Ref, Id, NewId, Value, self()]),
      {rejected, EtsRef, NewId};
    [{Ref, accepted, StoredValue}] ->
      ?LOG("[~p] Accept request rejected. Already agreed. Value ~p pid: ~p~n", [Ref, StoredValue, self()]),
      {already_agreed, StoredValue, EtsRef}
  end.


-spec sync(Ref :: term(), Value :: term(), State) -> ok
  when State :: term().
sync(Ref, Value, EtsRef) ->
  ?LOG("Sync request for: ~p with: ~p ~p~n", [Ref, Value, self()]),
  true = ets:insert(EtsRef, {Ref, accepted, Value}),
  ok.

-spec get(Ref :: term(), State) -> {ok, term()} | not_found
  when State :: term().
get(Ref, EtsRef) ->
  ?LOG("Get request for: ~p ~p~n", [Ref, self()]),
  case ets:lookup(EtsRef, Ref) of
    [{Ref, accepted, Value}] ->
      {ok, Value};
    _ ->
      not_found
  end.
