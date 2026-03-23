defmodule SPQueue.MixProject do
  use Mix.Project

  @github_url "https://github.com/svenvc/spqueue"

  def project do
    [
      app: :spqueue,
      version: "0.1.2",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # hex
      package: package(),
      description: "A Simple Persistent Queue",

      # docs
      name: "SPQueue",
      source_url: @github_url,
      docs: docs()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.34", only: :dev, runtime: false, warn_if_outdated: true}
    ]
  end

  defp docs do
    [
      main: "SPQueue",
      extras: ["README.md"]
    ]
  end

  defp package do
    [
      files: ~w(lib test script mix.exs README.md LICENSE),
      licenses: ["MIT"],
      links: %{
        "GitHub" => @github_url
      }
    ]
  end
end
