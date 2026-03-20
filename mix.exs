defmodule SPQueue.MixProject do
  use Mix.Project

  def project do
    [
      app: :spqueue,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # hex
      package: package(),
      description: "A Simple Persistent Queue",

      # docs
      name: "SPQueue",
      source_url: "https://github.com/svenvc/spqueue",
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
      licenses: "MIT",
      links: %{
        "GitHub" => "https://github.com/svenvc/spqueue"
      }
    ]
  end
end
