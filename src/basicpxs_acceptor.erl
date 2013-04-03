%%% Paxos Acceptor

-module(basicpxs_acceptor).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("basicpxs_config.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, 
         code_change/3]).

-record(state, {
            
            ballot_num = {0, 0},
            
            % Set of pvalues where
            % pvalue = {Ballot number, Slot number, Proposal}
            accepted = sets:new()
}).

-define(SELF, self()).

%% ====================================================================
%% External functions
%% ====================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

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
init([]) ->
    ?LINFO("START::Acceptor:~p", [self()]),
    {ok, #state{}}.

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
%% phase 1 a message from some leader
%% --------------------------------------------------------------------
handle_cast({p1a, {Leader, LBallot}}, #state{ballot_num = CurrBallot,
                                          accepted = Accepted} = State) ->
    ?LINFO("Acceptor:~p:: Received P1A ~p from ~p", [self(), Leader, LBallot]),
    case basicpxs_util:ballot_greater(LBallot, CurrBallot) of
        true ->
            Ballot = LBallot;
        false ->
            Ballot = CurrBallot
    end,
    NewState = State#state{ballot_num = Ballot},
    % Response = {p1b, self(), ballot_num, accepted} /From paper
    Response = {p1b, {?SELF, Ballot, Accepted}},
    gen_server:cast(Leader, Response),
    {noreply, NewState};
%% --------------------------------------------------------------------
%% phase 2 a message from some leader
%% --------------------------------------------------------------------
handle_cast({p2a, {Leader, {LBallot, _Slot, _Proposal} = PValue}},
            #state{ballot_num = CurrBallot, accepted = Accepted} = State) ->
    ?LINFO("Acceptor:~p:: Received P2A ~p from ~p", [self(), PValue, Leader]),
    case basicpxs_util:ballot_greateq(LBallot, CurrBallot) of
        true ->
            Ballot = LBallot,
            NewAccepted = sets:add_element(PValue, Accepted);
        false ->
            Ballot = CurrBallot,
            NewAccepted = Accepted
    end,
    NewState = State#state{ballot_num = Ballot, accepted = NewAccepted},
    % Response = {p2b, self(); ballot num} /From paper
    Response = {p2b, {?SELF, Ballot}},
    gen_server:cast(Leader, Response),
    {noreply, NewState};
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
