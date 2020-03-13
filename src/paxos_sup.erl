%%%-------------------------------------------------------------------
%% @doc paxos top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(paxos_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
  SupFlags = #{strategy => one_for_one,
    intensity => 10,
    period => 10},
  ChildSpecs = [
    #{id => paxos_acceptor_manager_sup,
      start => {paxos_acceptor_manager_sup, start_link, []},
      restart => permanent,
      type => supervisor,
      modules => [paxos_acceptor_manager_sup, paxos_acceptor]
    },
    #{id => paxos_acceptor_manager,
      start => {paxos_acceptor_manager, start_link, []},
      restart => permanent,
      type => worker,
      shutdown => 3000,
      modules => [paxos_acceptor_manager]
    }
  ],
  {ok, {SupFlags, ChildSpecs}}.

%% internal functions
