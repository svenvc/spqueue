defmodule SPQueueWorker do
  use GenServer

  @moduledoc """
  A worker to handle/process messages on a persistent queue.

  You specify the `queue_name` of the `SPQueue` to work with.
  This is an atom name of a registered process that is looked up each time.

  The `handler_function` takes a message from the queue as argument
  and must return `:ack` or `:nack` after which the message is dequeued.

  Additionally, each `periodic_interval` the queue will be processed.

  The worker should be set as delegate in the `SPQueue`,
  so that it receives `:enqueued` notification messages
  whenever items have been added to the queue.

  Each time `handle_queue_messages` is called, be it from the `:enqueued`
  notification of the queue or from the `:periodic_check`, all messages
  will be handled until the queue is empty.

  Since the worker is in its own process, it can take its time as needed.
  """

  defstruct queue_name: "pq",
            handler_function: nil,
            periodic_interval: :timer.seconds(5)

  @doc """
  Initialize an `SPQueueWorker` process's `GenServer` state

  The following options are provided:
  - `name`: the worker process' name
  - `queue_name`: the name of the persistent PQ to work with
  - `handler_function`: function that takes a message to process and returns :ack or :nack
  - `periodic_interval`: trigger period processing each interval
  """
  @impl true
  def init(opts) do
    name = Keyword.get(opts, :queue_name)
    handler_function = Keyword.get(opts, :handler_function)
    periodic_interval = Keyword.get(opts, :periodic_interval)

    initial_state =
      %__MODULE__{}
      |> Map.update!(:queue_name, fn default -> name || default end)
      |> Map.update!(:handler_function, fn default -> handler_function || default end)
      |> Map.update!(:periodic_interval, fn default -> periodic_interval || default end)

    Process.send_after(self(), :periodic_check, initial_state.periodic_interval)

    {:ok, initial_state}
  end

  @doc """
  Start an `SPQueueWorker` as a linked process

  See `init/1` for the list of options
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  @doc """
  Start an `SPQueueWorker` process without linking

  See `init/1` for the list of options
  """
  def start(opts) do
    GenServer.start(__MODULE__, opts, opts)
  end

  @doc """
  Stop an `SPQueueWorker` process
  """
  def stop(pq) do
    GenServer.stop(pq)
  end

  @impl true
  def handle_info(:enqueued, state) do
    new_state = handle_queue_messages(state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:periodic_check, state) do
    new_state = handle_queue_messages(state)
    Process.send_after(self(), :periodic_check, state.periodic_interval)
    {:noreply, new_state}
  end

  defp handle_queue_messages(state) do
    pq = Process.whereis(state.queue_name)

    if pq do
      drain_queue(pq, state.handler_function)
    end

    state
  end

  defp drain_queue(pq, handler_function) do
    if !SPQueue.empty?(pq) do
      message = SPQueue.head(pq)

      case apply(handler_function, [message]) do
        :ack -> SPQueue.dequeue(pq, ack: true)
        :nack -> SPQueue.dequeue(pq, ack: false)
      end

      drain_queue(pq, handler_function)
    end
  end
end
