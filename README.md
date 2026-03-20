# SPQueue

A Simple Persistent Queue for Elixir.

This is a FIFO queue: you add/enqueue items/messages at the end,
and you remove/dequeue items/messages from the start/head.

The queue is persistent on disk, using log files in a named directory.

## Installation

The package can be installed
by adding `spqueue` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:spqueue, "~> 0.1.0"}
  ]
end
```

The docs can be found at <https://hexdocs.pm/spqueue>.
