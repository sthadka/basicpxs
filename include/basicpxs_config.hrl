%%% Configs and macros

%% Logging macros

-define(LDEBUG(Msg),
    lager:debug(Msg)).
-define(LDEBUG(Msg, Args),
    lager:debug(Msg, Args)).

-define(LINFO(Msg),
    lager:info(Msg)).
-define(LINFO(Msg, Args),
    lager:info(Msg, Args)).

-define(LNOTICE(Msg),
    lager:notice(Msg)).
-define(LNOTICE(Msg, Args),
    lager:notice(Msg, Args)).

-define(LWARNING(Msg),
    lager:warning(Msg)).
-define(LWARNING(Msg, Args),
    lager:warning(Msg, Args)).

-define(LERROR(Msg),
    lager:error(Msg)).
-define(LERROR(Msg, Args),
    lager:error(Msg, Args)).

-define(LCRITICAL(Msg),
    lager:critical(Msg)).
-define(LCRITICAL(Msg, Args),
    lager:critical(Msg, Args)).

-define(LALERT(Msg),
    lager:alert(Msg)).
-define(LALERT(Msg, Args),
    lager:alert(Msg, Args)).

-define(LEMERGENCY(Msg),
    lager:emergency(Msg)).
-define(LEMERGENCY(Msg, Args),
    lager:emergency(Msg, Args)).


%% Names of actors - Currently unused
% TODO: Clean up

%% -define(CLIENT, basicpxs_client).
%% -define(REPLICA, basicpxs_replica).
%% -define(ACCEPTOR, basicpxs_acceptor).
%% -define(LEADER, basicpxs_leader).
%% -define(COMMANDER, basicpxs_commander).
%% -define(SCOUT, basicpxs_scout).