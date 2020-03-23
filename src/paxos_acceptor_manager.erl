%%%-------------------------------------------------------------------
%%% @author tihanyipeter
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(paxos_acceptor_manager).
-author("tihanyipeter").

-behaviour(gen_server).

%% API
-export([ start_link/0,
          get_acceptors/0,
          stop_acceptor/1,
          get_acceptors_by_node/1,
          start_acceptor/1]).

%% gen_server callbacks
-export([ init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2,
          code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {acceptors = [] :: list(),
                majority = 1 :: pos_integer(),
                down_nodes = [] :: [pid()]
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_acceptor(module()) -> pid().
start_acceptor(Module) ->
  gen_server:call(?SERVER, {start_acceptor, Module}).

-spec get_acceptors() ->
  {Majority :: non_neg_integer(), [pid()]}.
get_acceptors() ->
  gen_server:call(?MODULE, get_acceptors).

-spec get_acceptors_by_node(node()) ->
  [pid()].
get_acceptors_by_node(Node) ->
  gen_server:call(?MODULE, {get_acceptors_by_node, Node}).

-spec stop_acceptor(pid()) ->
  ok | term().
stop_acceptor(Pid) ->
  gen_server:call(?MODULE, {stop_acceptor, Pid}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  Nodes = nodes(),
  ok = net_kernel:monitor_nodes(true),
  lists:foreach(fun(N) ->
                    {?MODULE, N} ! {new_node, node()}
                end, Nodes),
  {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call(get_acceptors, _From, #state{acceptors = Acceptors, majority = M} = State) ->
  {reply, {M, lists:map(fun({_, Pid}) -> Pid end, Acceptors)}, State};
handle_call({get_acceptors_by_node, Node}, _From, #state{acceptors = Acceptors} = State) ->
  {reply, get_pids_by_node(Node, Acceptors), State};
handle_call({start_acceptor, Module}, _From, #state{acceptors = Acceptors} = State) ->
  Pid = paxos_acceptor_manager_sup:start_acceptor(Module, lists:map(fun({_, Pid}) -> Pid end, Acceptors)),
  gen_server:multi_call(nodes(), ?MODULE, {join, node(), Pid}),
  NewState = State#state{acceptors = [{node(), Pid} | Acceptors]},
  {reply, Pid, count_total(NewState)};
handle_call({stop_acceptor, Pid}, From, State) when node(Pid) /= node() ->
  % acceptor is not handled by this node
  Node = node(Pid),
  case net_adm:ping(Node) of
    pong ->
      try
        {reply, rpc:call(node(Pid), ?MODULE, stop_acceptor, [Pid], 3000), State}
      catch _Class:Error ->
        {reply, Error, State}
      end;
    _ ->
      % manager remote node is not reachable start coordinating remove on it's own.
      gen_server:multi_call((nodes() -- [Node]), ?MODULE, {leave, Node, Pid}),
      handle_call({leave, Node, Pid}, From, State)
  end;
handle_call({stop_acceptor, Pid}, _From, #state{acceptors = Acceptors} = State) ->
  NewList = lists:filter(fun({_, CPid}) -> CPid =/= Pid end, Acceptors),
  paxos_acceptor_manager_sup:stop_acceptor(Pid),
  gen_server:multi_call(nodes(), ?MODULE, {leave, node(), Pid}),
  NewState = State#state{acceptors = NewList},
  {reply, ok, count_total(NewState)};
handle_call({join, Node, Pid}, _From, #state{acceptors = A} = State) ->
  NewState = State#state{acceptors = [{Node, Pid} | A]},
  {reply, ok, count_total(NewState)};
handle_call({leave, _Node, Pid}, _From, #state{acceptors = A} = State) ->
  NewA = lists:filter(fun({_, CPid}) -> CPid =/= Pid end, A),
  NewState = State#state{acceptors = NewA},
  {reply, ok, count_total(NewState)};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({exchange, Node, List}, #state{acceptors = Acceptors} = State) ->
  NewList = lists:filter(fun({Cnode, _}) -> Cnode =/= Node end, Acceptors),
  NewList2 = lists:foldl(fun(Pid, Acc) -> [{Node, Pid} | Acc] end, NewList, List),
  NewState = State#state{acceptors = NewList2},
  {noreply, count_total(NewState)};
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info({nodeup, Node}, #state{down_nodes = Dn, acceptors = A} =State) ->
  send_exchange(Node, node(), get_local_pids(A)),
  {noreply, State#state{down_nodes = lists:delete(Node, Dn)}};
handle_info({nodedown, Node}, #state{down_nodes = Dn, acceptors = Acceptors} = State) ->
  case get_pids_by_node(Node, Acceptors) of
    [] ->
      % Not seen any pid from this node, maybe we out of sync
      NewList2 = get_pid_for_remote_not_from_remote_nodes(nodes(), Node, Acceptors),
      NewState = State#state{acceptors = NewList2, down_nodes = [Node | Dn]},
      {noreply, count_total(NewState)};
    _ ->
      {noreply, State#state{down_nodes = [Node | Dn]}}
  end;
handle_info({new_node, Node}, #state{down_nodes = Dn, acceptors = A} = State) ->
  send_exchange(Node, node(), get_local_pids(A)),
  [send_exchange(Node, DownNode, get_pids_by_node(DownNode, A)) || DownNode <- Dn],
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, #state{acceptors = Acceptors} = _State) ->
  LocalAcceptors = get_local_pids(Acceptors),
  [gen_server:multi_call(nodes(), ?MODULE, {leave, self(), Pid}) || Pid <- LocalAcceptors],
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
count_total(#state{acceptors = Acceptors} = State) ->
  Total = length(Acceptors),
  Majority = (Total div 2) +1 ,
  State#state{majority = Majority}.

get_local_pids(Acceptors) ->
  get_pids_by_node(node(), Acceptors).

get_pids_by_node(Node, Acceptors) ->
  lists:foldl(fun({CNode, Pid}, Acc) when CNode == Node -> [Pid | Acc];
                  (_, Acc) -> Acc end, [], Acceptors).


send_exchange(TargetNode, Node, Pids) ->
  gen_server:cast(
    {?MODULE, TargetNode},
    {exchange, Node, Pids}
  ).


get_pid_for_remote_not_from_remote_nodes([], _DownNode, Acc) ->
  Acc;
get_pid_for_remote_not_from_remote_nodes([Node | RestOfNodes], DownNode, Acceptors) ->
  RemoteAcceptorList = rpc:call(Node, ?MODULE, get_acceptors_by_node, [DownNode]),
  NewAcc = lists:foldl(fun(Pid, Acc) ->
                            Item = {DownNode, Pid},
                            case lists:member(Item, Acc) of
                              true -> Acc;
                              _ -> [Item | Acc]
                            end
                         end, Acceptors, RemoteAcceptorList),
  get_pid_for_remote_not_from_remote_nodes(RestOfNodes, DownNode, NewAcc).