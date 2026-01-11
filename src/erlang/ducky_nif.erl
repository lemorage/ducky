-module(ducky_nif).
-export([connect/1, close/1, execute_query/3, test/0]).
-on_load(init/0).

init() ->
    SoName = case code:priv_dir(ducky) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true ->
                    filename:join(["..", priv, native, ducky_nif]);
                false ->
                    filename:join([priv, native, ducky_nif])
            end;
        Dir ->
            filename:join([Dir, native, ducky_nif])
    end,
    erlang:load_nif(SoName, 0).

connect(_Path) ->
    erlang:nif_error(nif_not_loaded).

close(_Connection) ->
    erlang:nif_error(nif_not_loaded).

execute_query(_Connection, _Sql, _Params) ->
    erlang:nif_error(nif_not_loaded).

test() ->
    erlang:nif_error(nif_not_loaded).
