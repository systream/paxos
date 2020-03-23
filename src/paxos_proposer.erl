%%%-------------------------------------------------------------------
%%% @author tihanyipeter
%%% @copyright (C) 2020, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(paxos_proposer).
-author("tihanyipeter").

-define(DEFAULT_TIMEOUT, 100).

%-define(LOG(F, A), ct:pal(F, A)).
-define(LOG(_F, _A), ok).

-export([get/2, get/1, put/2]).

-spec get(Id :: term()) ->
  {ok ,term()} | {error, not_enough_acceptors} | not_found.
get(Id) ->
  {_, AcceptorList} = paxos_acceptor_manager:get_acceptors(),
  get(Id, length(AcceptorList), AcceptorList).

-spec get(Id :: term(), NumberOfProposers :: pos_integer()) ->
  {ok ,term()} | {error, not_enough_acceptors} | not_found.
get(Id, Num) ->
  {_, AcceptorList} = paxos_acceptor_manager:get_acceptors(),
  get(Id, Num, AcceptorList).

-spec get(Id :: term(), NumberOfProposers :: pos_integer(), [pid()]) ->
  {ok ,term()} | {error, not_enough_acceptors} | not_found | ignore.
get(_Key, _Num, []) ->
  {error, not_enough_acceptors};
get(_Key, Num, AcceptorList) when length(AcceptorList) < Num ->
  {error, not_enough_acceptors};
get(Key, Num, AcceptorList) when Num >= 1 ->
  ?LOG("* Start get request Key: ~p Num: ~p~n", [Key, Num]),
  send_get_request(Key, AcceptorList, Num),
  Majority = (Num div 2) + 1,
  receive_object(Key, #{}, Majority, Num).


-spec put(term(), term()) ->
  {already_agreed, term()} | accepted | {error, not_enough_acceptors} | timeout.
put(Key, Value) ->
  Self = self(),
  Ref = make_ref(),
  Pid = spawn_link(fun() -> Self ! {Ref, put(Key, Value, 1)} end),
  receive
    {Ref, Result} ->
      Result
  after 3000 ->
    exit(Pid, normal),
    timeout
  end.

put(Key, Value, Id) ->
  AcceptorList = paxos_acceptor_manager:get_acceptors(),
  {_, _AcceptorPids} = AcceptorList,
  ?LOG("* Count of acceptors: ~p Key: ~p Id: ~p Pid: ~p~n", [length(_AcceptorPids), Key, Id, self()]),
  case prepare(AcceptorList, Key, Id) of
    accepted ->
      ?LOG("* Prepare accepted. Continue with accept phase~n", []),
      case accept(AcceptorList, Key, Id, Value) of
        {already_agreed, Value} ->
          accepted;
        {rejected, RId} ->
          maybe_sleep(RId),
          NewId = RId+rand:uniform(3),
          ?LOG("* Prepare rejected rety with new id: ~p~n", [NewId]),
          put(Key, Value, NewId);
        timeout ->
          maybe_sleep(Id),
          NewId = Id+rand:uniform(3),
          ?LOG("* Prepare rejected dute to timeout rety with new id: ~p~n", [NewId]),
          put(Key, Value, NewId);
        Else ->
          Else
      end;
    {rejected, RId} ->
      maybe_sleep(RId),
      NewId = RId+rand:uniform(3),
      ?LOG("* Prepare rejected rety with new id: ~p~n", [NewId]),
      put(Key, Value, NewId);
    {already_agreed, Value} ->
      accepted;
    timeout ->
      maybe_sleep(Id),
      NewId = Id+rand:uniform(3),
      ?LOG("* Prepare rejected dute to timeout rety with new id: ~p~n", [NewId]),
      put(Key, Value, NewId);
    Else ->
      Else
  end.

receive_object(Key, Stat, Majority, NotReplied) ->
  receive
    {get_response, Key, Value} ->
      ?LOG("Got get response for ~p with: ~p ~n", [Key, Value]),
      NewCount = maps:get(Value, Stat, 0) + 1,
      NewStat = #{Value => NewCount},
      ?LOG("Stat: ~p~nMajority: ~p~n", [NewStat, Majority]),
      case NewCount >= Majority of
        false ->
          receive_object(Key, NewStat, Majority, NotReplied-1);
        _ ->
          io:format(user, "Stat: ~p~n", [NewStat]),
          Value
      end
  after ?DEFAULT_TIMEOUT ->
    case stat_majority_check(Stat, Majority) of
      not_found ->
        ignore;
      Else ->
        Else
    end
  end.

stat_majority_check(Stat, Majority) ->
  maps:fold(fun (_, _, Value) when Value =/= not_found ->
                  Value;
                (Value, Count, not_found) when Count >= Majority ->
                  Value;
                (_Value, _Count, not_found) ->
                  not_found
            end, not_found, Stat).

send_get_request(_Key, _AcceptorList, 0) ->
  ok;
send_get_request(_Key, AcceptorList, Num) when length(AcceptorList) < Num ->
  {error, not_enough_acceptors};
send_get_request(Key, AcceptorList, Num) ->
  Acceptor = lists:nth(rand:uniform(length(AcceptorList)), AcceptorList),
  paxos_acceptor:send_get_request(Acceptor, Key),
  ?LOG("Sent get request to ~p with key: ~p~n", [Acceptor, Key]),
  send_get_request(Key, lists:delete(Acceptor, AcceptorList), Num-1).

prepare({_, []}, _Key, _Id) ->
  ?LOG("Prepare request: Not enough acceptors available: num acceptors: ~p~n",
    [0]),
  {error, not_enough_acceptors};
prepare({Majority, AcceptorList}, Key, Id) ->
  [paxos_acceptor:prepare_request(Pid, Id, Key) || Pid <- AcceptorList],
  ?LOG("Prepare request sent: Majority is ~p Key: ~p Id: ~p~nWaiting for replys~n", [Majority, Key, Id]),
  receive_reply(Majority, Key, Id).

accept({_, []}, _Key, _Id, _Value) ->
  ?LOG("Accept request: Not enough acceptors available: num acceptors: ~p~n",
    [0]),
  {error, not_enough_acceptors};
accept({Majority, AcceptorList}, Key, Id, Value) ->
  [paxos_acceptor:accept_request(Pid, Id, Key, Value) || Pid <- AcceptorList],
  receive_reply(Majority, Key, Id).


receive_reply(Majority, Key, Id) ->
  receive_reply(#{accepted => 0, rejected => 0, already_agreed => {0, not_set}, max_id => 0}, Majority, Key, Id).

receive_reply(#{accepted := Accepted}, Majority, _Key, _Id) when Accepted >= Majority ->
  accepted;
receive_reply(#{rejected := Rejected, max_id := Id}, Majority, _Key, _Id) when Rejected >= Majority ->
  {rejected, Id};
receive_reply(#{already_agreed := {Agreed, Value}}, Majority, _Key, _Id) when Agreed >= Majority ->
  {already_agreed, Value};
receive_reply(Stat, Majority, Key, Id) ->
  receive
    {accepted, Id, Key} ->
      ?LOG("Accepted received: Key: ~p Id: ~p~n", [Key, Id]),
      #{accepted := Accepted} = Stat,
      receive_reply(Stat#{accepted => Accepted+1}, Majority, Key, Id);
    {promise, Id, Key} ->
      ?LOG("Promise received: Key: ~p Id: ~p~n", [Key, Id]),
      #{accepted := Accepted} = Stat,
      receive_reply(Stat#{accepted => Accepted+1}, Majority, Key, Id);
    {reject, Id, Key, NewId} ->
      ?LOG("Reject received: Key: ~p NewId: ~p Id: ~p~n", [Key, NewId, Id]),
      #{rejected := Rejected, max_id := OldId} = Stat,
      MaxId = case OldId < NewId of
                true -> NewId;
                _ -> OldId
              end,
      receive_reply(Stat#{rejected => Rejected+1, max_id => MaxId}, Majority, Key, Id);
    {already_agreed, Id, Key, Value} ->
      ?LOG("Already agreed received: Key: ~p~n", [Key]),
      #{already_agreed := {Agreed, _}} = Stat,
      receive_reply(Stat#{already_agreed => {Agreed+1, Value}}, Majority, Key, Id);
    _Msg ->
      ?LOG("Flush late msg: ~p~n", [_Msg]),
      % flush late msgs
      receive_reply(Stat, Majority, Key, Id)
  after ?DEFAULT_TIMEOUT ->
    ?LOG("Rejected due to timeout: ~p~n", [Key]),
    timeout
  end.


maybe_sleep(Id) when is_number(Id) ->
  case Id rem 10 of
    0 ->
      timer:sleep(rand:uniform(30));
    _ ->
      ok
  end.