%%% Paxos Client

-module(basicpxs_client).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include_lib("basicpxs_config.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/1, propose/1, get_replicas/0, set_replicas/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, 
         code_change/3]).

-record(state, {
            % List of replicas known to the client
            replicas = [],
            
            % Process that calls the client
            call_proc,
            
            % Unique id that represents the client
            cid             
}).

-define(SELF, self()).

%% ====================================================================
%% External functions
%% ====================================================================

start_link(Replicas) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Replicas], []).

set_replicas(Replicas) ->
    gen_server:call(?MODULE, {set_replicas, Replicas}).

get_replicas() ->
    gen_server:call(?MODULE, {get_replicas}).

%% Operation need to be a anon function that will be performed on the state
%  Operation needs to have return format = {Result, NewTLog} 
propose(Operation) ->
    gen_server:call(?MODULE, {propose, Operation}).

%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init([Replicas]) ->
    ?LINFO("START::Client:~p", [self()]),
    {ok, #state{replicas = Replicas}}.

%% --------------------------------------------------------------------
%% Get list of replicas avaiable to the client
%% --------------------------------------------------------------------
handle_call({get_replicas}, _From, #state{replicas = Replicas} = State) ->
    {reply, Replicas, State};
%% --------------------------------------------------------------------
%% Set list of replicas avaiable to the client
%% --------------------------------------------------------------------
handle_call({set_replicas, Replicas}, _From, State) ->
    {reply, ok, State#state{replicas = Replicas}};
%% Cast a message to all replicas with the proposal
%% --------------------------------------------------------------------
handle_call({propose, Operation}, From, #state{replicas = Replicas} = State) ->
    ?LINFO("Proposing message to replicas. ~p", [Operation]),
    % Proposal = {K, Cid, Operation}
    % Cnode = unique client identifier
    % Cid = client local unique command identifier
    % Operation = that is to be performed on the STATE of the system
    Cid = make_ref(),
    Proposal = {?SELF, Cid, Operation},
    Message = {request, Proposal},
    basicpxs_util:multiplecast(Replicas, Message),
    {reply, ok, State#state{call_proc = From, cid = Cid}}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(Msg, State) ->
    receiver ! Msg,
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------
