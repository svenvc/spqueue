defmodule SPQueue do
  use GenServer

  @moduledoc """
  SPQueue is a persistent FIFO queue.

  You add or `enqueue/2` items at the end of the queue.
  You remove or `dequeue/2` items from the head of the queue.

  This is a persistent queue: it stores its contents on files
  in a directory `base_dir`/`name` so that it can recover after a
  restart or failure, and can grow without using lots of memory.

  Items should be maps than can be converted to `JSON`.

  ## Examples

      iex> SPQueue.start(name: :my_queue)
      iex> ~w(a b c) |> Enum.each(fn c -> SPQueue.enqueue(:my_queue, %{"what" => c}) end)
      iex> SPQueue.count(:my_queue)
      3
      iex> SPQueue.dequeue(:my_queue)
      %{"what" => "a"}
      iex> SPQueue.head(:my_queue)
      %{"what" => "b"}
      iex> SPQueue.reset(:my_queue)
      iex> SPQueue.stop(:my_queue)

  ## Implementation

  The implementation splits the queue in a number of segments.
  In total, there can maximally be `number_of_segments` segments.
  Each segment can maximally contain `segment_size` items or messages.
  The whole queue is limited to a maximum size of `number_of_segments` * `segment_size`.

  Only one or two segments are kept in memory, the `first_segment`
  where dequeue is happening and possible enqueue when there is only
  one segment. If there is more than one segment, `last_segment`
  is where enqueue is happening. In the case of more than two segments,
  the intervening ones are not kept in memory and are loaded as needed.

  For each segment there are two files: an enqueue and a dequeue file,
  with a segment_id in their name. Both are appended to only, like a log.
  These files have .ndjson as extension, newline delimited JSON.
  When segments are both fully enqueued and dequeued, their files are
  cleaned up to reclaim disk space.

  For each session, an enqueue and dequeue count are kept.
  This count is also used as an internal id, along with a timestamp.

  Both enqueue and dequeue are O(1).
  Segments use Erlang's queue module for their implementation.

  ## Using internal IDs

  The client API has a number of functions with a `_r` suffix that
  return this meta information. One way this can be useful is to do
  a `head_r/1`, try to process an item, and then dequeue it on the condition
  that the id is still the same.

  ## Delegate

  Optionally, a `delegate` can be specified, a process that will be sent
  the `:enqueued` message after each enqueue operation. The delegate can
  be a pid or an atom name of a registered process which is looked up each time.
  """

  # state structure

  @default_segment_size 10
  @default_number_of_segments 5

  defstruct name: "pq",
            base_dir: File.cwd!(),
            segment_size: @default_segment_size,
            number_of_segments: @default_number_of_segments,
            enqueue_count: 0,
            dequeue_count: 0,
            first_segment: :queue.new(),
            last_segment: :queue.new(),
            first_segment_id: 0,
            last_segment_id: 0,
            delegate: nil

  # initialization

  @doc """
  Initialize an `SPQueue` process's `GenServer` state

  The following options are provided:
  - `name`: the name of the queue (an atom), also used as name for the genserver and the sub directory
  - `base_dir`: the base path under which queue sub directories will be created
  - `segment_size`: the maximum number of items in a segment
  - `number_of_segments`: the maximum number of segments
  - `delegate`: the process to send :enqueued when items are added
  """
  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name)
    base_dir = Keyword.get(opts, :base_dir)
    segment_size = Keyword.get(opts, :segment_size)
    number_of_segments = Keyword.get(opts, :number_of_segments)
    delegate = Keyword.get(opts, :delegate)

    initial_state =
      %__MODULE__{}
      |> Map.update!(:name, fn default -> name || default end)
      |> Map.update!(:base_dir, fn default -> base_dir || default end)
      |> Map.update!(:segment_size, fn default -> segment_size || default end)
      |> Map.update!(:number_of_segments, fn default -> number_of_segments || default end)
      |> Map.update!(:delegate, fn default -> delegate || default end)
      |> load_state_from_disk()

    {:ok, initial_state}
  end

  @doc """
  Start an `SPQueue` as a linked process

  See `init/1` for the list of options
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  @doc """
  Start an `SPQueue` process without linking

  See `init/1` for the list of options
  """
  def start(opts) do
    GenServer.start(__MODULE__, opts, opts)
  end

  @doc """
  Stop an `SPQueue` process
  """
  def stop(pq) do
    GenServer.stop(pq)
  end

  # client API

  @doc """
  Enqueue `msg` on queue `pq`, i.e. add it at the end.

  The queue is identified by a pid or a genserver name.
  `Msg` should be a map that can be `JSON` encoded.

  Returns `msg` on success and `nil` when the queue is full.

  ## Examples

      iex> {:ok, q} = SPQueue.start([])
      iex> SPQueue.enqueue(q, %{"test" => 42})
      %{"test" => 42}
      iex> SPQueue.head(q)
      %{"test" => 42}
      iex> SPQueue.reset(q)
      iex> SPQueue.stop(q)

  """
  def enqueue(pq, msg) when is_map(msg) do
    case enqueue_r(pq, msg) do
      {:ok, record} -> record["msg"]
      {:error, :full} -> nil
    end
  end

  @doc """
  Enqueue msg on queue pq, i.e. add it at the end.

  The queue is identified by a pid or a genserver name.
  Msg should be a map that can be `JSON` encoded.

  Returns `{:ok, %{"id" => id, "ts" => ts, "msg" => msg}}` on success.

  Return `{:error, :full}` when the queue is full.

  ## Examples

      iex> {:ok, q} = SPQueue.start([])
      iex> {:ok, %{"id" => id, "ts" => _ts, "msg" => msg}} = SPQueue.enqueue_r(q, %{"test" => 42})
      iex> id
      0
      iex> msg
      %{"test" => 42}
      iex> {:ok, %{"id" => ^id, "ts" => _ts, "msg" => ^msg}} = SPQueue.dequeue_r(q, id: id)
      iex> SPQueue.reset(q)
      iex> SPQueue.stop(q)

  """
  def enqueue_r(pq, msg) when is_map(msg) do
    GenServer.call(pq, {:enqueue, msg})
  end

  @doc """
  Dequeue a msg from queue `pq`, i.e. remove it from the head.

  The queue is identified by a pid or a genserver name.

  Returns a msg `map` on success, `nil` when the queue is empty.

  The boolean `ack:` option allows to make a difference between
  successful message consumption or message rejection.
  Optionally an `id:` can be specified for the expected internal id,
  which can be obtained from `enqueue_r/2` or `head_r/1`.
  If the `id:` does not match, `nil` is returned and no dequeue happens.

  ## Examples

      iex> {:ok, q} = SPQueue.start([])
      iex> SPQueue.enqueue(q, %{"test" => 42})
      iex> SPQueue.dequeue(q)
      %{"test" => 42}
      iex> SPQueue.dequeue(q)
      nil
      iex> SPQueue.reset(q)
      iex> SPQueue.stop(q)

  """
  def dequeue(pq, opts \\ [ack: true]) do
    case dequeue_r(pq, opts) do
      {:ok, record} -> record["msg"]
      {:error, :empty} -> nil
      {:error, :mismatch} -> nil
    end
  end

  @doc """
  Dequeue a msg from queue pq, i.e. remove it from the head.

  The queue is identified by a pid or a genserver name.

  Returns `{:ok, %{"id" => id, "ts" => ts, "msg" => msg}}` on success,

  Returns `{:error, :empty}` when the queue is empty.

  The boolean `ack:` option allows to make a difference between
  successful message consumption or message rejection.
  Optionally an `id:` can be specified for the expected internal id,
  which can be obtained from `enqueue_r/2` or `head_r/1`.
  If the `id:` does not match, `{:error, :mismatch}` is returned and no dequeue happens.

  ## Examples

      iex> {:ok, q} = SPQueue.start([])
      iex> {:ok, %{"id" => id, "ts" => _ts, "msg" => msg}} = SPQueue.enqueue_r(q, %{"test" => 42})
      iex> id
      0
      iex> msg
      %{"test" => 42}
      iex> {:ok, %{"id" => ^id, "ts" => _ts, "msg" => ^msg}} = SPQueue.dequeue_r(q, id: id)
      iex> SPQueue.reset(q)
      iex> SPQueue.stop(q)

  """
  def dequeue_r(pq, opts \\ [ack: true]) do
    GenServer.call(pq, {:dequeue, Keyword.merge([ack: true], opts)})
  end

  @doc """
  Return the head of queue `pq`, the message that would be the result of dequeue,
  without actually removing it.

  Return `nil` if the queue is empty.

  The queue is identified by a pid or a genserver name.

  ## Examples

      iex> {:ok, q} = SPQueue.start([])
      iex> SPQueue.enqueue(q, %{"test" => 42})
      iex> SPQueue.head(q)
      %{"test" => 42}
      iex> SPQueue.dequeue(q)
      iex> SPQueue.head(q)
      nil
      iex> SPQueue.reset(q)
      iex> SPQueue.stop(q)

  """
  def head(pq) do
    case head_r(pq) do
      {:ok, record} -> record["msg"]
      {:error, :empty} -> nil
    end
  end

  @doc """
  Return the head of queue `pq`, the message that would be the result of `dequeue_r/2`,
  without actually removing it.

  The queue is identified by a pid or a genserver name.

  Returns `{:ok, %{"id" => id, "ts" => ts, "msg" => msg}}` on success.

  `id` is the internal identification that can be used in `dequeue_r/2`
  to make sure the same message is removed that was read with `head_r/1`.

  Return `{:error, :empty}` if the queue is empty.

  ## Examples

      iex> {:ok, q} = SPQueue.start([])
      iex> SPQueue.enqueue(q, %{"test" => 42})
      iex> {:ok, %{"id" => id, "ts" => _ts, "msg" => msg}} = SPQueue.head_r(q)
      iex> id
      0
      iex> msg
      %{"test" => 42}
      iex> {:ok, %{"id" => ^id, "ts" => _ts, "msg" => ^msg}} = SPQueue.dequeue_r(q, id: id)
      iex> SPQueue.reset(q)
      iex> SPQueue.stop(q)

  """
  def head_r(pq) do
    GenServer.call(pq, :head)
  end

  @doc """
  Return whether queue `pq` is empty or not.

  The queue is identified by a pid or a genserver name.
  """
  def empty?(pq) do
    count(pq) == 0
  end

  @doc """
  Return the number of items or messages in queue `pq`.

  The queue is identified by a pid or a genserver name.
  """
  def count(pq) do
    GenServer.call(pq, :count)
  end

  @doc """
  Reset queue `pq` to an empty state, both in memory and on disk.

  With the `clean` options, the queue's base dir is removed as well.
  This is the default.

  The queue is identified by a pid or a genserver name.
  """
  def reset(pq, opts \\ [clean: true]) do
    GenServer.call(pq, {:reset, opts})
  end

  @doc """
  Return a map with information about queue `pq`.

  This includes the value of all options described in `init/1`
  and the following properties:
  - `queue_base_dir`: the full path to the directory where the queue's files are stored
  - `maximum_size`: the total maxium size of the queue
  - `count`: the number of items/messages in the queue

  The queue is identified by a pid or a genserver name.
  """
  def info(pq) do
    GenServer.call(pq, :info)
  end

  # server API

  @impl true
  def handle_call({:enqueue, msg}, {_sender, _call}, state) do
    if full?(state) do
      {:reply, {:error, :full}, state}
    else
      # this is the record that we write to the enqueue file
      record = %{
        "id" => state.enqueue_count,
        "ts" => to_string(DateTime.utc_now()),
        "msg" => msg
      }

      # make sure JSON encoding succeeds
      json = JSON.encode_to_iodata!(record)

      # now append to the enqueue log file
      log_enqueue(state, json)

      # update the in memory state
      new_state =
        case segments_count(state) do
          1 ->
            # there is only 1 segment, i.e. first and last are the same, only first is used
            if state.enqueue_count + 1 == (state.first_segment_id + 1) * state.segment_size do
              # first_segment will be full after this, make sure last_segment will be used next time
              %{
                state
                | enqueue_count: state.enqueue_count + 1,
                  first_segment: :queue.in(record, state.first_segment),
                  last_segment: :queue.new(),
                  last_segment_id: state.last_segment_id + 1
              }
            else
              # normal addition to first_segment
              %{
                state
                | enqueue_count: state.enqueue_count + 1,
                  first_segment: :queue.in(record, state.first_segment)
              }
            end

          _ ->
            if state.enqueue_count + 1 == (state.last_segment_id + 1) * state.segment_size do
              # last_segment will be full after this, arrange for a new last_segment to be used next time
              # there is no need to actually add to the in-memory segment as we start a new one anyway
              %{
                state
                | enqueue_count: state.enqueue_count + 1,
                  last_segment: :queue.new(),
                  last_segment_id: state.last_segment_id + 1
              }
            else
              # normal addition to last_segment
              %{
                state
                | enqueue_count: state.enqueue_count + 1,
                  last_segment: :queue.in(record, state.last_segment)
              }
            end
        end

      {:reply, {:ok, record}, notify_delegate(new_state)}
    end
  end

  @impl true
  def handle_call({:dequeue, opts}, {_sender, _call}, state) do
    cond do
      queue_empty?(state) ->
        {:reply, {:error, :empty}, state}

      Keyword.has_key?(opts, :id) &&
          Keyword.get(opts, :id) != :queue.head(state.first_segment)["id"] ->
        {:reply, {:error, :mismatch}, state}

      true ->
        # get a hold of the head without making modifications
        head = :queue.head(state.first_segment)

        # this is the record that we write to the dequeue file
        record = %{
          "id" => head["id"],
          "ts" => to_string(DateTime.utc_now()),
          "ack" => Keyword.get(opts, :ack)
        }

        # make sure JSON encoding succeeds
        json = JSON.encode_to_iodata!(record)

        # now append to the dequeue log file
        log_dequeue(state, json)

        # update the in memory state
        {{:value, head}, rest} = :queue.out(state.first_segment)

        new_state =
          if :queue.is_empty(rest) && segments_count(state) > 1 do
            # more than 1 segment is in use and it will be empty afterwards
            case segments_count(state) do
              2 ->
                # exactly 2 segments in use, shift the last one to the first,
                # empty the last, and clean up files
                %{
                  state
                  | first_segment: state.last_segment,
                    first_segment_id: state.last_segment_id,
                    last_segment: :queue.new(),
                    dequeue_count: state.dequeue_count + 1
                }
                |> gc_unused_segments()

              _ ->
                # more the 2 segments in use, load a new segment as first one
                # and clean up files
                new_segment_id = state.first_segment_id + 1

                %{
                  state
                  | first_segment: :queue.from_list(load_segment(state, new_segment_id)),
                    first_segment_id: new_segment_id,
                    dequeue_count: state.dequeue_count + 1
                }
                |> gc_unused_segments()
            end
          else
            # only first segment is in used, normal removal
            %{
              state
              | first_segment: rest,
                dequeue_count: state.dequeue_count + 1
            }
          end

        {:reply, {:ok, head}, new_state}
    end
  end

  @impl true
  def handle_call(:head, {_sender, _call}, %__MODULE__{first_segment: first_segment} = state) do
    if queue_empty?(state) do
      {:reply, {:error, :empty}, state}
    else
      head = :queue.head(first_segment)
      {:reply, {:ok, head}, state}
    end
  end

  @impl true
  def handle_call(:count, {_sender, _call}, state) do
    {:reply, queued_count(state), state}
  end

  @impl true
  def handle_call({:reset, opts}, {_sender, _call}, state) do
    base_dir_path = queue_base_dir(state)

    if Keyword.get(opts, :clean, true) do
      File.rm_rf!(base_dir_path)
    else
      File.ls!(base_dir_path)
      |> Enum.filter(fn file -> Path.extname(file) == ".ndjson" end)
      |> Enum.map(fn file -> File.rm!(Path.join(base_dir_path, file)) end)
    end

    {:reply, true,
     %{
       state
       | enqueue_count: 0,
         dequeue_count: 0,
         first_segment: :queue.new(),
         last_segment: :queue.new(),
         first_segment_id: 0,
         last_segment_id: 0
     }}
  end

  @impl true
  def handle_call(:info, {_sender, _call}, state) do
    info =
      Map.take(state, [:base_dir, :name, :segment_size, :number_of_segments, :delegate])
      |> Map.put(:count, queued_count(state))
      |> Map.put(:queue_base_dir, queue_base_dir(state))
      |> Map.put(:maximum_size, maximum_size(state))

    {:reply, info, state}
  end

  # internals

  defp queued_count(
         %__MODULE__{enqueue_count: enqueue_count, dequeue_count: dequeue_count} = _state
       ) do
    enqueue_count - dequeue_count
  end

  defp segments_count(
         %__MODULE__{
           first_segment_id: first_segment_id,
           last_segment_id: last_segment_id
         } = _state
       ) do
    last_segment_id - first_segment_id + 1
  end

  defp queue_empty?(state) do
    queued_count(state) == 0
  end

  defp full?(state) do
    queued_count(state) >= maximum_size(state)
  end

  defp maximum_size(
         %__MODULE__{segment_size: segment_size, number_of_segments: number_of_segments} = _state
       ) do
    segment_size * number_of_segments
  end

  defp load_state_from_disk(state) do
    base_dir_path = queue_base_dir(state)

    files =
      base_dir_path
      |> File.ls!()
      |> Enum.filter(fn name -> String.match?(name, ~r/^.*.ndjson$/) end)

    # find the last (highest id) dequeue segment file

    last_dequeue_file =
      files
      |> Enum.filter(fn name -> String.match?(name, ~r/^dequeue-.*.ndjson$/) end)
      |> Enum.max_by(fn name -> segment_id_from_file(name) end, fn -> nil end)

    last_dequeue_segment_id =
      if last_dequeue_file, do: segment_id_from_file(last_dequeue_file), else: 0

    # get the last dequeued id, which equals the dequeue count

    dequeue_count =
      if last_dequeue_file,
        do: (read_last_ndjson(Path.join(base_dir_path, last_dequeue_file)) |> Map.get("id")) + 1,
        else: 0

    # find the last (highest id) enqueue segment file

    last_enqueue_file =
      files
      |> Enum.filter(fn name -> String.match?(name, ~r/^enqueue-.*.ndjson$/) end)
      |> Enum.max_by(fn name -> segment_id_from_file(name) end, fn -> nil end)

    last_enqueue_segment_id =
      if last_enqueue_file, do: segment_id_from_file(last_enqueue_file), else: 0

    # load the whole segment

    last_enqueue_segment =
      if last_enqueue_file,
        do: :queue.from_list(load_segment(state, last_enqueue_segment_id)),
        else: :queue.new()

    # the enqueue count equals the last id

    enqueue_count =
      if :queue.is_empty(last_enqueue_segment),
        do: 0,
        else: (:queue.last(last_enqueue_segment) |> Map.get("id")) + 1

    if last_dequeue_segment_id == last_enqueue_segment_id do
      # we're in the situation where there is only 1 segment,
      # stored in the first segment while the last segment is empty,
      # execute the necessary dequeues
      {_, last_enqueue_segment} =
        :queue.split(
          rem(dequeue_count, state.segment_size),
          last_enqueue_segment
        )

      %{
        state
        | enqueue_count: enqueue_count,
          dequeue_count: dequeue_count,
          first_segment: last_enqueue_segment,
          last_segment: :queue.new(),
          first_segment_id: last_dequeue_segment_id,
          last_segment_id: last_enqueue_segment_id
      }
    else
      # we're in the at least 2 segments situation
      # load the last segment and execute the necessary dequeues
      {_, last_dequeue_segment} =
        :queue.split(
          rem(dequeue_count, state.segment_size),
          :queue.from_list(load_segment(state, last_dequeue_segment_id))
        )

      %{
        state
        | enqueue_count: enqueue_count,
          dequeue_count: dequeue_count,
          first_segment: last_dequeue_segment,
          last_segment: last_enqueue_segment,
          first_segment_id: last_dequeue_segment_id,
          last_segment_id: last_enqueue_segment_id
      }
    end
  end

  defp load_segment(state, segment_id) do
    path = Path.join(queue_base_dir(state), "enqueue-#{segment_id}.ndjson")
    File.stream!(path) |> Enum.map(fn line -> JSON.decode!(line) end)
  end

  defp log_enqueue(
         %__MODULE__{enqueue_count: enqueue_count, segment_size: segment_size} = state,
         json
       ) do
    segment_id = div(enqueue_count, segment_size)
    path = Path.join(queue_base_dir(state), "enqueue-#{segment_id}.ndjson")
    append_ndjson(json, path)
  end

  defp log_dequeue(
         %__MODULE__{dequeue_count: dequeue_count, segment_size: segment_size} = state,
         json
       ) do
    segment_id = div(dequeue_count, segment_size)
    path = Path.join(queue_base_dir(state), "dequeue-#{segment_id}.ndjson")
    append_ndjson(json, path)
  end

  defp gc_unused_segments(%__MODULE__{first_segment_id: first_segment_id} = state) do
    base_dir_path = queue_base_dir(state)

    # all segment files (enqueue & dequeue) with id less than
    # the id of the current first segment are no longer needed

    File.ls!(base_dir_path)
    |> Enum.filter(fn file -> Path.extname(file) == ".ndjson" end)
    |> Enum.filter(fn file -> segment_id_from_file(file) < first_segment_id end)
    |> Enum.map(fn file -> File.rm!(Path.join(base_dir_path, file)) end)

    # note that if the current segment is full and then fully read out,
    # all log files will be removed. if the queue would then be restarted,
    # it will start its internal id counting from 0 again.
    # as this is an internal id, this should make no functional difference

    state
  end

  defp notify_delegate(state) do
    delegate = state.delegate

    if delegate do
      send(delegate, :enqueued)
    end

    state
  end

  defp queue_base_dir(%__MODULE__{name: name, base_dir: base_dir} = _state) do
    path = Path.join(base_dir, to_string(name))

    if !File.exists?(path) do
      File.mkdir_p!(path)
    end

    path
  end

  defp segment_id_from_file(name) do
    Path.rootname(name) |> String.split("-") |> List.last() |> String.to_integer()
  end

  # IO support

  defp read_last_ndjson(file) do
    File.stream!(file) |> Enum.reduce(nil, fn line, _last -> line end) |> JSON.decode!()
  end

  defp append_ndjson(io_data, file) do
    File.write!(file, [io_data, "\n"], [:append])
  end
end
