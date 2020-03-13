%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(paxos_acceptor).
-author("tihanyipeter").

-callback init() ->
  {ok, State :: term()}.

-callback prepare(Id :: pos_integer(), Ref :: term(), State) ->
  {accepted, State} |
  {rejected, ActualId :: pos_integer(), State} |
  {already_agreed, Value :: term(), State}
  when State :: term().

-callback accept(Id :: pos_integer(), Ref :: term(), Value :: term(), State) ->
  {accepted, State} |
  {rejected, ActualId :: pos_integer(), State} |
  {already_agreed, Value :: term(), State}
  when State :: term().

-callback sync(Ref :: term(), Value :: term(), State) -> ok
  when State :: term().

-callback get(Ref :: term(), State) -> {ok, term()} | not_found
  when State :: term().


%% API
-export([ start_link/1,
          prepare_request/3,
          accept_request/4,
          get/3,
          sync/3,
          send_get_request/2,
          loop/2]).


-spec prepare_request(pid(), pos_integer(), term()) -> ok.
prepare_request(Pid, Id, Key) when Id > 0 ->
  Pid ! {prepare, Id, Key, self()},
  ok.

-spec accept_request(pid(), pos_integer(), term(), term()) -> ok.
accept_request(Pid, Id, Key, Value) when Id > 0 ->
  Pid ! {accept, Id, Key, Value, self()},
  ok.

-spec send_get_request(pid(), term()) -> ok.
send_get_request(Pid, Key) ->
  Pid ! {get, Key, self()},
  ok.

-spec get(pid(), term(), non_neg_integer() | infinity) -> term() | not_found.
get(Pid, Key, Timeout) ->
  send_get_request(Pid, Key),
  receive
    {get_response, Key, Result} ->
      Result
  after Timeout ->
    timeout
  end.

-spec sync(pid(), term(), term()) -> ok.
sync(Pid, Key, Value) ->
  Pid ! {sync, Key, Value},
  ok.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(module()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Module) ->
  Self = self(),
  Ref = make_ref(),
  Pid = proc_lib:spawn(fun() ->
                            {ok, State} = Module:init(),
                            Self ! {paxos_started, Module, Ref},
                            loop(Module, State)
                         end),
  receive
    {paxos_started, Module, Ref} ->
      link(Pid),
      {ok, Pid}
  after 5000 ->
    exit(Pid, kill),
    {error, timeout}
  end.


-spec loop(module(), term()) -> no_return().
loop(Module, State) ->
  receive
    {get, Key, SenderPid} ->
      SenderPid ! {get_response, Key, Module:get(Key, State)},
      ?MODULE:loop(Module, State);
    {sync, Key, Value} ->
      Module:sync(Key, Value, State),
      ?MODULE:loop(Module, State);
    {prepare, Id, Key, SenderPid} ->
      case Module:prepare(Id, Key, State) of
        {accepted, NewState} ->
          SenderPid ! {promise, Id, Key},
          ?MODULE:loop(Module, NewState);
        {rejected, NewState, NewId} ->
          SenderPid ! {reject, Id, Key, NewId},
          ?MODULE:loop(Module, NewState);
        {already_agreed, Value, NewState} ->
          SenderPid ! {already_agreed, Id, Key, Value},
          ?MODULE:loop(Module, NewState)
      end;
    {accept, Id, Key, Value, SenderPid} ->
      case Module:accept(Id, Key, Value, State) of
        {accepted, NewState} ->
          SenderPid ! {accepted, Id, Key},
          ?MODULE:loop(Module, NewState);
        {rejected, NewState, NewId} ->
          SenderPid ! {reject, Id, Key, NewId},
          ?MODULE:loop(Module, NewState);
        {already_agreed, StoredValue, NewState} ->
          SenderPid ! {already_agreed, Id, Key, StoredValue},
          ?MODULE:loop(Module, NewState)
      end;
    _M ->
      %io:format("Unkown msg: ~p ~p~n", [_M, self()]),
      ?MODULE:loop(Module, State)
  end.
