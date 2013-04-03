%%% Paxos Leader

-module(basicpxs_leader).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("basicpxs_config.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/1,
         get_acceptors/1, set_acceptors/2, get_replicas/1, set_replicas/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, 
         code_change/3]).

-define(SELF, self()).
-define(FIRST_BALLOT, {0, ?SELF}).

-record(state, {
            % Monotonically increasing unique ballot number
            ballot_num = ?FIRST_BALLOT,
            
            % State of the leader
            active = false,

            % A map of slot numbers to proposals in the form of a set
            % At any time, there is at most one entry per slot number in the set
            proposals = basicpxs_ht:init(),
            
            % List of acceptors
            acceptors = [],
            
            % List of replicas
            replicas = []
}).

%% ====================================================================
%% External functions
%% ====================================================================

start_link({Acceptors, Replicas}) ->
    gen_server:start_link(?MODULE, [{Acceptors, Replicas}], []).

set_replicas(Pid, Replicas) ->
    gen_server:call(Pid, {set_replicas, Replicas}).

get_replicas(Pid) ->
    gen_server:call(Pid, {get_replicas}).

set_acceptors(Pid, Acceptors) ->
    gen_server:call(Pid, {set_acceptors, Acceptors}).

get_acceptors(Pid) ->
    gen_server:call(Pid, {get_acceptors}).

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
init([{Acceptors, Replicas}]) ->
    ?LINFO("START::Leader:~p with args {~p, ~p}", 
           [self(), Acceptors, Replicas]),
    % Spawn scout with the first ballot
    {ok, _} = basicpxs_scout:start_link({?SELF, Acceptors, ?FIRST_BALLOT}),
    {ok, #state{acceptors = Acceptors,
                replicas = Replicas}}.

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
%% --------------------------------------------------------------------
%% Get list of acceptors avaiable to the client
%% --------------------------------------------------------------------
handle_call({get_acceptors}, _From, #state{acceptors = Acceptors} = State) ->
    {reply, Acceptors, State};
%% --------------------------------------------------------------------
%% Set list of acceptors avaiable to the client
%% --------------------------------------------------------------------
handle_call({set_acceptors, Acceptors}, _From, State) ->
    {reply, ok, State#state{acceptors = Acceptors}};
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
%% propse request from replica
%% --------------------------------------------------------------------
handle_cast({propose, {Slot, Proposal}}, 
            #state{proposals = Proposals, active = Active,
                   acceptors = Acceptors, replicas = Replicas,
                   ballot_num = Ballot} = State) ->
    ?LINFO("Leader:~p:: Received propose ~p", [self(), {Slot, Proposal}]),
    % Add the proposal if we do not have a command for the proposed spot
    case basicpxs_ht:valget(Slot, Proposals) of
        {ok, not_found} ->
            % Note: we could make Slot as the key (better performance), but just
            % following similar structure as replica to keep things consistent
            basicpxs_ht:set(Proposal, Slot, Proposals),
            case Active of
                true ->
                    PValue = {Ballot, Slot, Proposal},
                    {ok, _} = basicpxs_commander:start_link({?SELF, Acceptors,
                                                             Replicas, PValue});
                false ->
                    ok
            end;
        {ok, _Proposal} ->
            ok
    end,
    {noreply, State};
%% --------------------------------------------------------------------
%% adopted message sent by a scout,this message signifies that the current 
%% ballot number ballot num has been adopted by a majority of acceptors
%% Note: The adopted ballot_num has to match current ballot_num!
%% --------------------------------------------------------------------
handle_cast({adopted, {CurrBallot, PValues}}, 
            #state{proposals = Proposals, ballot_num = CurrBallot,
                   acceptors = Acceptors, replicas = Replicas} = State) ->
    ?LINFO("Leader:~p:: Received adopted ~p", [self(), {CurrBallot, sets:to_list(PValues)}]),
    % Get all the proposals in PValues with max ballot number and update our
    % proposals with this data
    Pmax = pmax(PValues),
    intersect(Proposals, Pmax),
    % Spawn a commander for every proposal
    spawn_commanders(CurrBallot, Proposals, Acceptors, Replicas),
    {noreply, State#state{active = true}};
%% --------------------------------------------------------------------
%% preempted message sent by either a scout or a commander, it means that some
%% acceptor has adopted some other ballot 
%% --------------------------------------------------------------------
handle_cast({preempted, ABallot}, 
            #state{ballot_num = CurrBallot, acceptors = Acceptors} = State) ->
    ?LINFO("Leader:~p:: Received preempted ~p", [self(), ABallot]),
    % If the new ballot number is bigger, increase ballot number and scout for
    % the next adoption
    case basicpxs_util:ballot_greater(ABallot, CurrBallot) of
        true ->
            NewBallot = basicpxs_util:incr_ballot(CurrBallot, ABallot),
            {ok, _} = basicpxs_scout:start_link({?SELF, Acceptors, NewBallot});
        false ->
            NewBallot = CurrBallot
    end,
    {noreply, State#state{active = false, ballot_num = NewBallot}};
% --------------------------------------------------------------------
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

% TODO: Naive implementation!! Make it better!
pmax(PValues) ->
    PList = sets:to_list(PValues),
    PDict = dict:new(),
    NewPDict = pmax(PList, PDict),
    NewPList = lists:map(fun({Key, Value}) -> get_obj(Key, Value) end,
                             dict:to_list(NewPDict)),
    sets:from_list(NewPList).
    
pmax([], PDict) ->
    PDict;
pmax([PVal|PList], PDict) ->
    {Key, Val} = get_keyval(PVal),
    case dict:find(Key, PDict) of
        % We already have a proposal with a different ballot num
        {ok, AltVal} ->
            % Get one with max ballot num
            case Val > AltVal of
                true ->
                    NewPDict = dict:store(Key, Val, PDict);
                false ->
                    NewPDict = PDict
            end;
        % Its a new proposal for the specified ballot
        error ->
            NewPDict = dict:store(Key, Val, PDict)
    end,
    pmax(PList, NewPDict).
                    
get_keyval({A, B, C}) ->
    {{B, C}, A}.

get_obj({B, C}, A) ->
    {A, B, C}.

% TODO: Try to make this faster
intersect(Proposals, MaxPValues) ->
    ?LINFO("NEWPMAX:: ->~p", [sets:to_list(MaxPValues)]),
    intersect_lst(Proposals, sets:to_list(MaxPValues)).

intersect_lst(_, []) ->
    ok;
intersect_lst(Proposals, [PVal|PList]) ->
    {_Ballot, Slot, Proposal} = PVal,
    basicpxs_ht:set(Proposal, Slot, Proposals),
    intersect_lst(Proposals, PList).
    
spawn_commanders(Ballot, Proposals, Acceptors, Replicas) ->
    spawn_commanders_lst(Ballot, basicpxs_ht:to_list(Proposals), 
                         Acceptors, Replicas).

spawn_commanders_lst(_Ballot, [], _Acceptors, _Replicas) ->
    ok;
spawn_commanders_lst(Ballot, [H|L], Acceptors, Replicas) ->
    {Proposal, Slot} = H,
    PValue = {Ballot, Slot, Proposal},
    {ok, _} = basicpxs_commander:start_link({?SELF, Acceptors, 
                                             Replicas, PValue}),
    spawn_commanders_lst(Ballot, L, Acceptors, Replicas).