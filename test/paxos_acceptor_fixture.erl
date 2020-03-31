%%%-------------------------------------------------------------------
%%% @author tihanyipeter
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Feb 2020 18:45
%%%-------------------------------------------------------------------
-module(paxos_acceptor_fixture).
-author("tihanyipeter").

-behavior(paxos_acceptor).


%% API
-export([prepare/3, init/0, accept/4, sync/3, get/2, fold/2]).

init() ->
  timer:sleep(6000),
  {error, stop}.

-spec prepare(Id :: pos_integer(), Ref :: term(), State) ->
  {accepted, State} |
  {rejected, ActualId :: pos_integer(), State} |
  {already_agreed, Value :: term(), State}
  when State :: term().
prepare(_NewId, _Ref, _EtsRef) ->
  erlang:error(not_implemented).

-spec accept(Id :: pos_integer(), Ref :: term(), Value :: term(), State) ->
  {accepted, State} |
  {rejected, ActualId :: pos_integer(), State} |
  {already_agreed, Value :: term(), State}
  when State :: term().
accept(_Id, _Ref, _Value, _EtsRef) ->
  erlang:error(not_implemented).

-spec sync(Ref :: term(), Value :: term(), State) -> ok
  when State :: term().
sync(_Ref, _Value, _EtsRef) ->
  erlang:error(not_implemented).

-spec get(term(), term()) -> ok.
get(_Ref, _State) ->
  erlang:error(not_implemented).

-spec fold(Fun, State) -> ok when
  Fun :: fun((Key :: term(), Value :: term()) -> term()),
  State :: term().
fold(_Fun, _State) ->
  erlang:error(not_implemented).