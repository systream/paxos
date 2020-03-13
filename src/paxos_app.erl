%%%-------------------------------------------------------------------
%% @doc paxos public API
%% @end
%%%-------------------------------------------------------------------

-module(paxos_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, _} = Result = paxos_sup:start_link(),
    Result.

stop(_State) ->
    ok.

%% internal functions
