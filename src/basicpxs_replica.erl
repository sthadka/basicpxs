%%% Paxos Replica

-module(basicpxs_replica).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("basicpxs_config.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/1, get_leaders/1, set_leaders/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, 
         code_change/3]).

-record(state, {
            % Set of leaders participating
            leaders = [],
            
            % Transaction log that is served to the client, using lists to keep
            % it simple; replace with O(1) looking data structure for
            % performance
            % This is the STATE described in the paper
            % TODO: Create behaviour?
            tlog = [],
            
            % Slot is the index of next item in tlog (for new decisions)
            slot_num = 1,
            
            % Minimum available open slot number in the set (for new proposals) 
            % (Proposals U Decisions)
            min_slot_num = 1,

            % A set of {slot number, command} pairs for proposals that the
            % replica has made in the past            
            proposals = basicpxs_ht:init(),
            
            % Another set of {slot number, command} pairs for decided slots
            decisions = basicpxs_ht:init()
}).

%% ====================================================================
%% External functions
%% ====================================================================

start_link(Leaders) ->
    gen_server:start_link(?MODULE, [Leaders], []).

set_leaders(Pid, Leaders) ->
    gen_server:call(Pid, {set_leaders, Leaders}).

get_leaders(Pid) ->
    gen_server:call(Pid, {get_leaders}).

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
init([Leaders]) ->
    ?LINFO("START::Replica:~p", [self()]),
    {ok, #state{leaders = Leaders}}.

%% --------------------------------------------------------------------
%% Get list of leaders avaiable to the client
%% --------------------------------------------------------------------
handle_call({get_leaders}, _From, #state{leaders = Leaders} = State) ->
    {reply, Leaders, State};
%% --------------------------------------------------------------------
%% Set list of leaders avaiable to the client
%% --------------------------------------------------------------------
handle_call({set_leaders, Leaders}, _From, State) ->
    {reply, ok, State#state{leaders = Leaders}};
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
%% Handle client's proposal
%% --------------------------------------------------------------------
handle_cast({request, Proposal}, State) ->
    ?LINFO("Replica:~p:: Received proposal ~p", [self(), Proposal]),
    {noreply, propose(Proposal, State)};
%% --------------------------------------------------------------------
%% Handle leader's decision
%% --------------------------------------------------------------------
handle_cast({decision, {Slot, Proposal}}, 
            #state{decisions = Decisions} = State) ->
    ?LINFO("Replica:~p:: Received decision ~p", [self(), {Slot, Proposal}]),
    % Add the decision to the set of decisions
    
    % A safety check to see if this is a duplicate decision
    case basicpxs_ht:keyget(Proposal, Decisions) of
        {ok, not_found} ->
            ok;
        {ok, _} ->
            ?LERROR("Received duplicate decision. Possible logical error")
    end,
        
    % Save the decision
    basicpxs_ht:set(Proposal, Slot, Decisions),
    % Go though decisions and update state if needed
    {noreply, check_decisions(State)};
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

propose(Proposal, #state{proposals = Proposals, decisions = Decisions, 
                         min_slot_num = MinSlot, leaders = Leaders} = State) ->
    % Check if there has been a decision of this proposal
    case basicpxs_ht:keyget(Proposal, Decisions) of
        % Get the next available slot number, add it to proposals and
        % let leaders know about it
        {ok, not_found} ->
            basicpxs_ht:set(Proposal, MinSlot, Proposals),
            Message = {propose, {MinSlot, Proposal}},
            basicpxs_util:multiplecast(Leaders, Message),
            NewMinSlot = MinSlot + 1,
            State#state{min_slot_num = NewMinSlot};
        % If already decided, ignore it
        _ ->
            State
    end.

perform({CNode, Cid, Operation}, #state{slot_num = CurrSlot, 
                                        tlog = TLog} = State) ->
    {Result, NewTLog} = Operation(TLog),
    Response = {reponse, {Cid, Result}},
    gen_server:cast(CNode, Response),
    State#state{tlog = NewTLog, slot_num = CurrSlot + 1}.
    
check_decisions(#state{proposals = Proposals, decisions = Decisions,
                       slot_num = CurrSlot} = State) ->
    % Check if we have a decision for the current slot
    case basicpxs_ht:valget(CurrSlot, Decisions) of
        % No decision for the current slot, nothing to change
        {ok, not_found} ->
            State;
        % We have a decision for the current slot
        {ok, CurrProposal} ->
            % Check if we have a proposal for this slot
            case basicpxs_ht:valget(CurrSlot, Proposals) of
                % We dont have a proposal for this slot
                {ok, not_found} ->
                    ?LINFO("Found decision without proposal"),
                    NewState = perform(CurrProposal, State),
                    check_decisions(NewState);
                % We have a proposal and it is the same as the decision
                {ok, CurrProposal} ->
                    NewState = perform(CurrProposal, State),
                    % We might have more decisions, check again
                    check_decisions(NewState);
                % We have a proposal, but it is different from the decision
                {ok, AltProposal} ->
                    % Re-propose it
                    UpdState = propose(AltProposal, State),
                    NewState = perform(CurrProposal, UpdState),
                    check_decisions(NewState)
            end
    end.