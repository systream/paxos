%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(paxos_acceptor_manager_sup).
-author("tihanyipeter").

-behaviour(supervisor).

-export([start_link/0, start_acceptor/1, stop_acceptor/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
  SupFlags = #{strategy => simple_one_for_one,
    intensity => 10,
    period => 100},
  ChildSpecs = [
    #{id => acceptor,
      start => {paxos_acceptor, start_link, []},
      restart => transient,
      shutdown => 3000,
      modules => [paxos_acceptor]
    }
  ],
  {ok, {SupFlags, ChildSpecs}}.

-spec start_acceptor(module()) -> pid().
start_acceptor(AcceptorModule) ->
  {ok, Pid} = supervisor:start_child(?MODULE, [AcceptorModule]),
  Pid.

-spec stop_acceptor(pid() | atom()) -> ok | {error, term()}.
stop_acceptor(Name) ->
  supervisor:terminate_child(?MODULE, Name).




