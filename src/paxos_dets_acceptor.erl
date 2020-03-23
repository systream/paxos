%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(paxos_dets_acceptor).
-author("tihanyipeter").

-behavior(paxos_acceptor).

%-define(LOG(F, A), io:format(F, A)).
-define(LOG(_F, _A), ok).
%-define(LOG(F, A), ct:pal(F, A)).

%% API
-export([prepare/3, init/0, accept/4, sync/3, get/2, fold/2]).

init() ->
  Filename = io_lib:format("~ts_acceptor.dets", [atom_to_list(node())]),
  {ok, _Ref} = dets:open_file(Filename, []).

-spec prepare(Id :: pos_integer(), Ref :: term(), State) ->
  {accepted, State} |
  {rejected, ActualId :: pos_integer(), State} |
  {already_agreed, Value :: term(), State}
  when State :: term().
prepare(NewId, Ref, DEtsRef) ->
  case dets:lookup(DEtsRef, Ref) of
    [] ->
      ok = dets:insert(DEtsRef, {Ref, prepare, NewId}),
      ?LOG("[~p] Prepare request accepted. NewId ~p LastId: No prev id pid: ~p ~n", [Ref, NewId, self()]),
      {accepted, DEtsRef};
    [{Ref, prepare, Id}] when NewId =< Id ->
      ?LOG("[~p] Prepare request rejected. NewId ~p LastId ~p pid: ~p ~n", [Ref, NewId, Id, self()]),
      {rejected, DEtsRef, Id};
    [{Ref, prepare, _Id}] ->
      ok = dets:insert(DEtsRef, {Ref, prepare, NewId}),
      ?LOG("[~p] Prepare request accepted. NewId ~p LastId: ~p pid: ~p~n", [Ref, NewId, _Id, self()]),
      {accepted, DEtsRef};
    [{Ref, accepted, Value}] ->
      ?LOG("[~p] Prepare request rejected. Already agreed. NewId ~p pid: ~p~n", [Ref, NewId, self()]),
      {already_agreed, Value, DEtsRef}
  end.


-spec accept(Id :: pos_integer(), Ref :: term(), Value :: term(), State) ->
  {accepted, State} |
  {rejected, ActualId :: pos_integer(), State} |
  {already_agreed, Value :: term(), State}
  when State :: term().
accept(Id, Ref, Value, DEtsRef) ->
  case dets:lookup(DEtsRef, Ref) of
    [{Ref, prepare, Id}] ->
      ?LOG("[~p] Accept request accepted. Id ~p Value ~p pid: ~p~n", [Ref, Id, Value, self()]),
      ok = dets:insert(DEtsRef, {Ref, accepted, Value}),
      {accepted, DEtsRef};
    [] ->
      ?LOG("[~p] Accept request rejected. No promise. Id ~p Value ~p pid: ~p~n", [Ref, Id, Value, self()]),
      {rejected, DEtsRef, 1};
    [{Ref, prepare, NewId}] ->
      ?LOG("[~p] Accept request rejected. Id ~p NewId: ~p Value ~p pid: ~p~n", [Ref, Id, NewId, Value, self()]),
      {rejected, DEtsRef, NewId};
    [{Ref, accepted, StoredValue}] ->
      ?LOG("[~p] Accept request rejected. Already agreed. Value ~p pid: ~p~n", [Ref, StoredValue, self()]),
      {already_agreed, StoredValue, DEtsRef}
  end.


-spec sync(Ref :: term(), Value :: term(), State) -> ok
  when State :: term().
sync(Ref, Value, DEtsRef) ->
  ?LOG("Sync request for: ~p with: ~p ~p~n", [Ref, Value, self()]),
  ok = dets:insert(DEtsRef, {Ref, accepted, Value}),
  ok.

-spec get(Ref :: term(), State) -> {ok, term()} | not_found
  when State :: term().
get(Ref, DEtsRef) ->
  ?LOG("Get request for: ~p ~p~n", [Ref, self()]),
  case dets:lookup(DEtsRef, Ref) of
    [{Ref, accepted, Value}] ->
      {ok, Value};
    _ ->
      not_found
  end.

-spec fold(Fun, State) -> ok when
  Fun :: fun((Key :: term(), Value :: term()) -> term()),
  State :: term().
fold(Fun, EtsRef) ->
  dets:foldl(fun({Ref, accepted, Value}, _Acc) ->
                  Fun(Ref, Value),
                  ok;
                (_, Acc) ->
                  Acc
            end, ok, EtsRef).