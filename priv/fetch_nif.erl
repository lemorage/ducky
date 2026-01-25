#!/usr/bin/env escript
%% -*- erlang -*-
%%! +sbtu +A1

%% Fetches platform-specific DuckDB NIF binary from GitHub releases

-mode(compile).

main(_Args) ->
    % Always self-locate using script path
    ScriptPath = escript:script_name(),
    PrivDir = filename:dirname(ScriptPath),
    % PrivDir is priv/ directory

    VersionFile = filename:join([PrivDir, "VERSION"]),
    Version = case filelib:is_file(VersionFile) of
        true ->
            read_version_file(VersionFile);
        false ->
            % Fallback: try gleam.toml in parent directory
            ProjectRoot = filename:dirname(PrivDir),
            GleamToml = filename:join([ProjectRoot, "gleam.toml"]),
            read_version(GleamToml)
    end,

    OutputPath = filename:join([PrivDir, "native", "ducky_nif" ++ extension()]),
    ManifestPath = filename:join([PrivDir, "ducky_nif", "Cargo.toml"]),

    % Skip if NIF already exists
    case filelib:is_file(OutputPath) of
        true ->
            halt(0);
        false ->
            inets:start(),
            ssl:start(),
            Platform = detect_platform(),
            BinaryName = binary_name(Platform),
            Url = "https://github.com/lemorage/ducky/releases/download/v" ++ Version ++ "/" ++ BinaryName,

            case download_and_extract(Url, OutputPath) of
                ok ->
                    halt(0);
                {error, _Reason} ->
                    % Silent fallback to source compilation
                    case compile_from_source(OutputPath, ManifestPath) of
                        ok ->
                            halt(0);
                        {error, CompileError} ->
                            io:format("ERROR: Failed to obtain DuckDB NIF: ~p~n", [CompileError]),
                            io:format("Platform: ~s~n", [Platform]),
                            halt(1)
                    end
            end
    end.

read_version_file(VersionPath) ->
    case file:read_file(VersionPath) of
        {ok, Content} ->
            string:trim(binary_to_list(Content));
        {error, Reason} ->
            io:format("ERROR: Could not read ~s: ~p~n", [VersionPath, Reason]),
            io:format("Package may be corrupted. Try: gleam clean && gleam build~n"),
            halt(1)
    end.

read_version(GleamTomlPath) ->
    case file:read_file(GleamTomlPath) of
        {ok, Content} ->
            Lines = string:split(binary_to_list(Content), "\n", all),
            case find_version_line(Lines) of
                {ok, Version} -> Version;
                not_found ->
                    io:format("ERROR: Could not find 'version =' in ~s~n", [GleamTomlPath]),
                    halt(1)
            end;
        {error, Reason} ->
            io:format("ERROR: Could not read ~s: ~p~n", [GleamTomlPath, Reason]),
            io:format("Package may be corrupted. Try: gleam clean && gleam build~n"),
            halt(1)
    end.

find_version_line([]) -> not_found;
find_version_line([Line | Rest]) ->
    Trimmed = string:trim(Line),
    case string:str(Trimmed, "version = ") of
        1 ->
            % Extract version from: version = "0.1.0"
            case string:split(Trimmed, "\"", all) of
                [_, Version, _] -> {ok, Version};
                _ -> find_version_line(Rest)
            end;
        _ -> find_version_line(Rest)
    end.

extension() ->
    case os:type() of
        {win32, _} -> ".dll";
        _ -> ".so"
    end.

binary_name(Platform) ->
    Ext = case string:str(Platform, "windows") > 0 of
        true -> ".dll";
        false -> ".so"
    end,
    "ducky_nif-" ++ Platform ++ Ext ++ ".gz".

detect_platform() ->
    OS = case os:type() of
        {unix, darwin} -> "darwin";
        {unix, linux} -> "linux";
        {win32, _} -> "windows"
    end,

    Arch = case erlang:system_info(system_architecture) of
        ArchStr when is_list(ArchStr) ->
            ArchLower = string:lowercase(ArchStr),
            HasAarch64 = string:str(ArchLower, "aarch64") > 0,
            HasArm64 = string:str(ArchLower, "arm64") > 0,
            case HasAarch64 orelse HasArm64 of
                true -> "aarch64";
                false -> "x86_64"
            end
    end,

    case {OS, Arch} of
        {"darwin", "aarch64"} -> "aarch64-apple-darwin";
        {"darwin", "x86_64"} -> "x86_64-apple-darwin";
        {"linux", "aarch64"} -> "aarch64-unknown-linux-gnu";
        {"linux", "x86_64"} -> "x86_64-unknown-linux-gnu";
        {"windows", "x86_64"} -> "x86_64-pc-windows-msvc";
        {O, A} -> io:format("WARNING: Unsupported platform ~s-~s~n", [O, A]), "unknown"
    end.

download_and_extract(Url, OutputPath) ->
    case httpc:request(get, {Url, []}, [{timeout, 120000}], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _Headers, CompressedBody}} ->
            try
                Body = zlib:gunzip(CompressedBody),
                filelib:ensure_dir(OutputPath),
                ok = file:write_file(OutputPath, Body),
                ok = file:change_mode(OutputPath, 8#755),
                ok
            catch
                _:Error -> {error, {extraction_failed, Error}}
            end;
        {ok, {{_, StatusCode, _}, _, _}} ->
            {error, {http_error, StatusCode}};
        {error, Reason} ->
            {error, {download_failed, Reason}}
    end.

compile_from_source(OutputPath, ManifestPath) ->
    case filelib:is_file(ManifestPath) of
        false -> {error, no_rust_source};
        true ->
            _Output = os:cmd("cargo build --release --manifest-path=" ++ ManifestPath ++ " 2>&1"),
            SourceLib = find_compiled_lib(ManifestPath),
            case filelib:is_file(SourceLib) of
                true ->
                    filelib:ensure_dir(OutputPath),
                    {ok, _} = file:copy(SourceLib, OutputPath),
                    ok = file:change_mode(OutputPath, 8#755),
                    ok;
                false ->
                    {error, build_failed}
            end
    end.

find_compiled_lib(ManifestPath) ->
    % ManifestPath is like: /path/to/priv/ducky_nif/Cargo.toml
    CargoDir = filename:dirname(ManifestPath),
    LibName = case os:type() of
        {unix, darwin} -> "libducky_nif.dylib";
        {unix, _} -> "libducky_nif.so";
        {win32, _} -> "ducky_nif.dll"
    end,
    filename:join([CargoDir, "target", "release", LibName]).
