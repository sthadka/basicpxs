%%% Paxos Scout

-module(basicpxs_scout).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("basicpxs_config.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, 
         code_change/3]).

-record(state, {
            % The leader that spawned this commander    
            leader,
            
            % Set of all acceptors
            acceptors = [],
            
            % Set of pvalues where
            % pvalue = {Ballot number, Slot number, Proposal}
            pvalues = sets:new(),
            
            % Ballot number
            ballot_num,
            
            % Number of acceptors that has agreed for this ballot
            vote_count
}).

-define(SELF, self()).

%% ====================================================================
%% External functions
%% ====================================================================

start_link({Leader, Acceptors, Ballot}) ->
    gen_server:start_link(?MODULE, [{Leader, Acceptors, Ballot}], []).


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
init([{Leader, Acceptors, Ballot}]) ->
    ?LINFO("START::Scout:~p", [self()]),
    ?LINFO("Scout:~p:: Sending message to acceptors ~p", [self(), Acceptors]),
    % Send a message to  all the acceptors and wait for their response
    Message = {p1a, {?SELF, Ballot}}, 
    basicpxs_util:multiplecast(Acceptors, Message),
    {ok, #state{leader = Leader,
                acceptors = Acceptors,
                ballot_num = Ballot}}.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% --------------------------------------------------------------------
%% phase 1 b message from some acceptor
%% --------------------------------------------------------------------
% TODO: We currently do not check the acceptor identity. Add check to make sure
% votes are only counted for unique acceptors
handle_cast({p1b, {_Acceptor, ABallot, APValues}}, 
            #state{ballot_num = CurrBallot, vote_count = VoteCount, 
                   acceptors = Acceptors, leader = Leader,
                   pvalues = PValues} = State) ->
    ?LINFO("Scout:~p:: Received P1B ~p", [self(), ABallot]),
    case basicpxs_util:ballot_equal(ABallot, CurrBallot) of
        true ->
            NewPValues = sets:union(APValues, PValues),
            case is_majority(VoteCount, Acceptors) of
                true ->
                    Message = {adopted, {ABallot, NewPValues}},
                    ?LINFO("MAJORITY:: to leader ~p", [Leader]),
                    gen_server:cast(Leader, Message),
                    {stop, normal, State};
                false ->
                    NewState = State#state{pvalues = NewPValues,
                                           vote_count = VoteCount + 1},
                    {noreply, NewState}
            end;
        false ->
            % We have another ballot running. Since acceptor will not send any
            % ballot smaller than what we have, we need not check explicitly.
            % Added it just to make sure!
            case basicpxs_util:ballot_lesser(ABallot, CurrBallot) of
                true ->
                    % TODO: Some issue with reason for init. Disable check for now
%%                     ?LERROR("Logic error! Smaller ballot received, ~p ~p", [ABallot, CurrBallot]),
%%                     {stop, logic_error, State};
                    {noreply, State};
                false ->
                    % We have a larger ballot; inform leader and exit
                    gen_server:cast(Leader, {preempted, ABallot}),
                    {stop, normal, State}
            end
    end;
%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(_Msg, State) ->
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

% Votes > (N/2 + 1)
is_majority(VoteCount, Acceptors) ->
    VoteCount > (erlang:trunc(erlang:length(Acceptors)) + 1).
