%%% Basic hash table

-module(basicpxs_ht).

-export([init/0, set/3, keyget/2, valget/2, to_list/1]).

init() ->
    ets:new(table, []).

set(Key, Value, Table) ->
    ets:insert(Table, {Key, Value}),
    ok.

keyget(Key, Table) ->
    case ets:lookup(Table, Key) of
        [] ->
            {ok, not_found};
        [{Key, Value}] ->
            {ok, Value};
        _ ->
            {error, unknown_object}
    end.

valget(Value, Table) ->
    case ets:select(Table, [{{'$1', '$2'},
                            [{'==', '$2', Value}],
                            [['$1']]}]) of
        [] ->
            {ok, not_found};
        [[Key]] ->
            {ok, Key};
        _ ->
            {error, unknown}
    end.

to_list(Table) ->
    ets:tab2list(Table).

%TODO: Code to delete objects and remove table