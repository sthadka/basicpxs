%%% Utility functions

-module(basicpxs_util).

-export([ballot_greateq/2, ballot_greater/2, ballot_lesser/2, incr_ballot/2, 
         ballot_equal/2, multiplecast/2]).

%% Ballot related functions
% TODO: Create types for frequently used structures

% Special case during init
ballot_greater({_IntA, _LeaderA}, {0, 0}) ->
    true;
% General case
ballot_greater({IntA, _LeaderA}, {IntB, _LeaderB}) ->
    IntA > IntB.
    
ballot_greateq({IntA, _LeaderA}, {IntB, _LeaderB}) ->
    IntA >= IntB.
    
ballot_lesser({IntA, _LeaderA}, {IntB, _LeaderB}) ->
    IntA >= IntB.

ballot_equal(BallotA, BallotB) ->
    BallotA =:= BallotB.
    
%% Assumes IntB > IntA
incr_ballot({_IntA, LeaderA}, {IntB, _LeaderB}) ->
    {IntB + 1, LeaderA}.


multiplecast([], _Message) ->
    ok;
multiplecast([Pid|Pids], Message) ->
    gen_server:cast(Pid, Message),
    multiplecast(Pids, Message).