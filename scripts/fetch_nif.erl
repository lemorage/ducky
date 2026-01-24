#!/usr/bin/env escript
%% -*- erlang -*-
%%! +sbtu +A1

%% Fetches platform-specific DuckDB NIF binary from GitHub releases

-mode(compile).

main(_Args) ->
    OutputPath = output_path(),

    % Skip if NIF already exists
    case filelib:is_file(OutputPath) of
        true ->
            halt(0);
        false ->
            inets:start(),
            ssl:start(),

            Version = read_version(),
            Platform = detect_platform(),
            BinaryName = binary_name(Platform),
            Url = "https://github.com/lemorage/ducky/releases/download/v" ++ Version ++ "/" ++ BinaryName,

            io:format("Acquiring DuckDB NIF for ~s...~n", [Platform]),

            case download_and_extract(Url, OutputPath) of
                ok ->
                    io:format("✓ Pre-built NIF ready~n"),
                    halt(0);
                {error, Reason} ->
                    io:format("Download failed: ~p~n", [Reason]),
                    io:format("Attempting to compile from source...~n"),
                    case compile_from_source(OutputPath) of
                        ok ->
                            io:format("✓ Compiled from source~n"),
                            halt(0);
                        {error, CompileError} ->
                            io:format("ERROR: ~p~n", [CompileError]),
                            halt(1)
                    end
            end
    end.

read_version() ->
    case file:read_file("gleam.toml") of
        {ok, Content} ->
            Lines = string:split(binary_to_list(Content), "\n", all),
            case find_version_line(Lines) of
                {ok, Version} -> Version;
                not_found ->
                    io:format("ERROR: Could not find 'version =' in gleam.toml~n"),
                    halt(1)
            end;
        {error, Reason} ->
            io:format("ERROR: Could not read gleam.toml: ~p~n", [Reason]),
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

output_path() ->
    % Output to package-local priv/native/, build system will copy to final location
    Base = "priv/native/ducky_nif",
    Ext = case os:type() of
        {win32, _} -> ".dll";
        _ -> ".so"
    end,
    Base ++ Ext.

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
    case httpc:request(get, {Url, []}, [{timeout, 30000}], [{body_format, binary}]) of
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

compile_from_source(OutputPath) ->
    ManifestPath = "priv/ducky_nif/Cargo.toml",
    case filelib:is_file(ManifestPath) of
        false -> {error, no_rust_source};
        true ->
            io:format("Building Rust NIF (this may take 2-3 minutes)...~n"),
            case os:cmd("cargo build --release --manifest-path=" ++ ManifestPath ++ " 2>&1") of
                Output ->
                    SourceLib = find_compiled_lib(),
                    case filelib:is_file(SourceLib) of
                        true ->
                            filelib:ensure_dir(OutputPath),
                            {ok, _} = file:copy(SourceLib, OutputPath),
                            ok = file:change_mode(OutputPath, 8#755),
                            ok;
                        false ->
                            io:format("Cargo output: ~s~n", [Output]),
                            {error, build_failed}
                    end
            end
    end.

find_compiled_lib() ->
    case os:type() of
        {unix, darwin} -> "priv/ducky_nif/target/release/libducky_nif.dylib";
        {unix, _} -> "priv/ducky_nif/target/release/libducky_nif.so";
        {win32, _} -> "priv/ducky_nif/target/release/ducky_nif.dll"
    end.
