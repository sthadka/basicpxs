-module(basicpxs_test).

-include_lib("eunit/include/eunit.hrl").

%%-------------------------------------------------------------------
%% setup code
%%-------------------------------------------------------------------
apps() ->
    %TODO: Lager fails if compiler, syntax_tools are not included. Find fix.
    [compiler, syntax_tools, lager].

app_start() ->
    start_receiver(),
    lists:foreach (fun (App) ->
                           case application:start (App) of
                               {error, {already_started, App}} -> ok;
                               ok -> ok;
                               Other ->
                                   erlang:error ({error,
                                                  {?MODULE, ?LINE,
                                                   'could not start',
                                                   App,
                                                   'reason was', Other}})
                           end
                   end,
                   apps ()),
    error_logger:tty(false).

app_stop(_) ->
    stop_receiver(),
    [ ?assertEqual(ok, application:stop(App)) || App <- lists:reverse(apps())],
    error_logger:tty(true).

start_receiver() ->
    register(receiver, spawn(fun() -> receiver(1) end)).

stop_receiver() ->
    whereis(receiver) ! stop.

receiver(Count) ->
   receive
       stop ->
           ok;
       Message ->
           ?debugFmt("<<<=== COUNT::~p::Message::~p~n", [Count, Message]),
           receiver(Count+1)
   end.

%%-------------------------------------------------------------------
%% test code
%%-------------------------------------------------------------------

basicpxs_test_() ->
    {timeout, 60,
     {setup,
      fun app_start/0,
      fun app_stop/1,
      [
       ?_test(simple_run())
      ]
     }}.

simple_run() ->
    ?debugMsg("Starting test"),
    % Start all the processes and update their state
    % Note: Order is important
    Acceptors = spawn_procs(acceptor, 3, null),
    Replicas = spawn_procs(replica, 1, []),
    Leaders = spawn_procs(leader, 1, {Acceptors, Replicas}),
    basicpxs_client:start_link([]),
    
    % Update procs that couldn't be initialized earlier
    basicpxs_client:set_replicas(Replicas),
    multi_exec(basicpxs_replica, set_leaders, Leaders, Replicas),

    Value = 10,
    Operation = fun(TLog) ->
                        NewTLog = [Value|TLog],
                        {success, NewTLog}
                end,

    ?debugFmt("Result: ~p ~n", [basicpxs_client:propose(Operation)]),
    
    timer:sleep(5000),
    
    ?assertEqual(1, 1).


%%-------------------------------------------------------------------
%% internal functions
%%-------------------------------------------------------------------

spawn_procs(Type, Count, Args) ->
    spawn_procs(Type, Count, Args, []).

spawn_procs(_Type, 0, _Args, Acc) ->
    Acc;
spawn_procs(Type, Count, Args, Acc) ->
    case Type of
        acceptor ->
            {ok, Pid} = basicpxs_acceptor:start_link();
        replica ->
            {ok, Pid} = basicpxs_replica:start_link(Args);
        leader ->
            {ok, Pid} = basicpxs_leader:start_link(Args)
    end,
    spawn_procs(Type, Count-1, Args, [Pid|Acc]).

multi_exec(_Mod, _Fun, _Val, []) ->
    ok;
multi_exec(Mod, Fun, Val, [Pid|Lst]) ->
    Mod:Fun(Pid, Val),
    multi_exec(Mod, Fun, Val, Lst).
