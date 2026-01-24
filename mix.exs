defmodule Ducky.MixProject do
  use Mix.Project

  def project do
    [
      app: :ducky,
      version: "0.1.0",
      elixir: "~> 1.14",
      erlc_paths: ["src", "build/dev/erlang"],
      compilers: Mix.compilers(),
      package: package(),
      deps: deps()
    ]
  end

  defp package do
    [
      files: ~w(
        src
        priv/ducky_nif
        scripts/fetch_nif.erl
        rebar.config
        gleam.toml
        LICENSE
        README.md
        CHANGELOG.md
      ),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/lemorage/ducky"}
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    []
  end
end
