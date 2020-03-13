Paxos
=====

Simple paxos protocol implementation.

## Starting/Stopping acceptors
```erlang
Pid1 = paxos:start_acceptor(),
Pid2 = paxos:start_acceptor(paxos_dets_acceptor).
```

Acceptors synced between nodes.

```erlang
paxos:stop_acceptor(Pid).
```

## Start a new consensus run

```erlang
paxos:put(foo, {"test", <<"bar">>}).
```

The result could be 
* `accepted` than it was a success run,
* `{already_agreed, TheAgreedValue}` when it was previously agreed on value 
* `timeout` when the consensus takes too much time,
* `{error, not_enough_acceptors}` when no one or not enough acceptor were run. 

## Get the value of previous consensus run

```erlang
paxos:get(foo).
```

The result could be 
* `{ok, Value}`,
* `not_found` when there is no known value,
* `timeout` when the request takes too much time,
* `{error, not_enough_acceptors}` when no one or not enough acceptor were run.


Build
-----

    $ rebar3 compile
    
Test
-----

    $ rebar3 ct --sname ct --cover true
