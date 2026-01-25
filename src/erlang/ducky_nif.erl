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

    case erlang:load_nif(SoName, 0) of
        ok ->
            ok;
        {error, {load_failed, _}} ->
            PrivDir = case code:priv_dir(ducky) of
                {error, bad_name} -> "priv";
                Dir2 -> Dir2
            end,
            FetchScript = filename:join([PrivDir, "fetch_nif.erl"]),
            _Result = os:cmd("escript " ++ FetchScript ++ " 2>&1"),
            % Retry loading
            case erlang:load_nif(SoName, 0) of
                ok ->
                    ok;
                {error, Reason} ->
                    io:format("ERROR: Failed to load DuckDB NIF: ~p~n", [Reason]),
                    io:format("Try running: escript " ++ FetchScript ++ "~n"),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

connect(_Path) ->
    erlang:nif_error(nif_not_loaded).

close(_Connection) ->
    erlang:nif_error(nif_not_loaded).

execute_query(_Connection, _Sql, _Params) ->
    erlang:nif_error(nif_not_loaded).

test() ->
    erlang:nif_error(nif_not_loaded).
