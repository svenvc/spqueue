defmodule SPQueue.Test do
  use ExUnit.Case, async: true
  doctest SPQueue

  test "initial state", %{line: line} = _context do
    pq = start_unique(line)
    assert(SPQueue.head(pq) == {:error, :empty})
    assert(SPQueue.count(pq) == 0)
    assert(SPQueue.empty?(pq))
    stop(pq)
  end

  test "simple enqueue/dequeue", %{line: line} = _context do
    pq = start_unique(line)

    1..3
    |> Enum.each(fn n ->
      {:ok, %{"msg" => %{"n" => ^n}, "id" => _id, "ts" => _ts}} = SPQueue.enqueue(pq, %{"n" => n})
    end)

    assert(SPQueue.count(pq) == 3)

    {:ok, %{"msg" => %{"n" => 1}, "id" => _id, "ts" => _ts}} = SPQueue.head(pq)

    1..3
    |> Enum.each(fn n ->
      {:ok, %{"msg" => %{"n" => ^n}, "id" => _id, "ts" => _ts}} = SPQueue.dequeue(pq)
    end)

    assert(SPQueue.empty?(pq))
    stop(pq)
  end

  test "simple enqueue/dequeue !", %{line: line} = _context do
    pq = start_unique(line)

    1..3 |> Enum.each(fn n -> SPQueue.enqueue!(pq, %{"n" => n}) end)

    assert(SPQueue.count(pq) == 3)

    assert(SPQueue.head!(pq) == %{"n" => 1})

    1..3 |> Enum.each(fn n -> assert(SPQueue.dequeue!(pq) |> Map.get("n") == n) end)

    assert(SPQueue.empty?(pq))
    stop(pq)
  end

  test "load state", %{line: line} = _context do
    pq = start_unique(line)
    1..3 |> Enum.each(fn n -> SPQueue.enqueue(pq, %{"n" => n}) end)
    SPQueue.dequeue(pq)
    assert(SPQueue.count(pq) == 2)
    assert(SPQueue.head!(pq) == %{"n" => 2})
    stop(pq, clean: false)
    pq = start_unique(line, clean: false)
    assert(SPQueue.count(pq) == 2)
    assert(SPQueue.head!(pq) == %{"n" => 2})
    stop(pq)
  end

  test "multiple segments 32/16", %{line: line} = _context do
    pq = start_unique(line)
    1..32 |> Enum.each(fn n -> SPQueue.enqueue(pq, %{"n" => n}) end)
    1..16 |> Enum.each(fn n -> assert(SPQueue.dequeue!(pq) |> Map.get("n") == n) end)
    assert(SPQueue.head!(pq) == %{"n" => 17})
    assert(SPQueue.count(pq) == 16)
    stop(pq)
  end

  test "multiple segments 40", %{line: line} = _context do
    pq = start_unique(line)
    1..40 |> Enum.each(fn n -> SPQueue.enqueue(pq, %{"n" => n}) end)
    assert(SPQueue.count(pq) == 40)
    1..40 |> Enum.each(fn n -> assert(SPQueue.dequeue!(pq) == %{"n" => n}) end)
    assert(SPQueue.empty?(pq))
    stop(pq)
  end

  test "load state multiple segments", %{line: line} = _context do
    pq = start_unique(line)
    1..32 |> Enum.each(fn n -> SPQueue.enqueue(pq, %{"n" => n}) end)
    1..16 |> Enum.each(fn n -> assert(SPQueue.dequeue!(pq) |> Map.get("n") == n) end)
    assert(SPQueue.head!(pq) == %{"n" => 17})
    assert(SPQueue.count(pq) == 16)
    stop(pq, clean: false)
    pq = start_unique(line, clean: false)
    assert(SPQueue.head!(pq) == %{"n" => 17})
    assert(SPQueue.count(pq) == 16)
    stop(pq)
  end

  test "options", %{line: line} = _context do
    queue_name = unique_name_for_test(line)
    tmp_dir = System.tmp_dir!()
    number_of_segments = 5
    segment_size = 20
    maximum_size = number_of_segments * segment_size

    {:ok, pq} =
      SPQueue.start(
        name: String.to_atom(queue_name),
        base_dir: tmp_dir,
        number_of_segments: number_of_segments,
        segment_size: segment_size
      )

    opts = SPQueue.info(pq)

    assert(opts[:count] == 0)
    assert(opts[:maximum_size] == maximum_size)
    assert(opts[:name] == String.to_atom(queue_name))
    assert(opts[:queue_base_dir] == Path.join(tmp_dir, queue_name))

    1..maximum_size |> Enum.each(fn i -> SPQueue.enqueue!(pq, %{"key" => i}) end)

    opts = SPQueue.info(pq)

    assert(opts[:count] == maximum_size)

    1..maximum_size |> Enum.each(fn i -> assert(SPQueue.dequeue!(pq) == %{"key" => i}) end)

    opts = SPQueue.info(pq)

    assert(opts[:count] == 0)

    stop(pq)
  end

  test "full", %{line: line} = _context do
    pq = start_unique(line)
    max = SPQueue.info(pq)[:maximum_size]
    1..max |> Enum.each(fn n -> SPQueue.enqueue!(pq, %{"n" => n}) end)
    refute(SPQueue.empty?(pq))
    assert(SPQueue.count(pq) == max)
    assert(SPQueue.enqueue(pq, %{"n" => max + 1}) == {:error, :full})
    stop(pq)
  end

  test "gc", %{line: line} = _context do
    pq = start_unique(line)

    0..4
    |> Enum.each(fn round ->
      1..40 |> Enum.each(fn i -> {:ok, _} = SPQueue.enqueue(pq, %{"n" => round * 40 + i}) end)
      assert(SPQueue.count(pq) == 40)
      1..40 |> Enum.each(fn _i -> {:ok, _} = SPQueue.dequeue(pq) end)
      assert(SPQueue.empty?(pq))
    end)

    assert(Enum.empty?(queue_base_dir(pq) |> File.ls!()))

    stop(pq)
  end

  test "harmonica", %{line: line} = _context do
    pq = start_unique(line)

    [{40, 10}, {10, 30}, {20, 10}, {10, 30}, {20, 20}, {15, 5}, {15, 5}, {10, 15}, {10, 25}]
    |> Enum.reduce({0, 0}, fn {to_enqueue, to_dequeue}, {total_enqueued, total_dequeued} ->
      total_enqueued_next = total_enqueued + to_enqueue

      total_enqueued..(total_enqueued_next - 1)
      |> Enum.each(fn n -> SPQueue.enqueue!(pq, %{"n" => n}) end)

      total_dequeued_next = total_dequeued + to_dequeue

      total_dequeued..(total_dequeued_next - 1)
      |> Enum.each(fn _n -> SPQueue.dequeue!(pq) end)

      {total_enqueued_next, total_dequeued_next}
    end)

    assert(Enum.empty?(queue_base_dir(pq) |> File.ls!()))
    stop(pq)
  end

  test "dequeue with id", %{line: line} = _context do
    pq = start_unique(line)
    100..105 |> Enum.each(fn n -> {:ok, _record} = SPQueue.enqueue(pq, %{"code" => n}) end)
    {:ok, _record} = SPQueue.dequeue(pq)
    {:ok, %{"id" => id} = _record} = SPQueue.head(pq)
    {:ok, _record} = SPQueue.dequeue(pq, id: id)
    {:error, :mismatch} = SPQueue.dequeue(pq, id: id)
    1..4 |> Enum.each(fn i -> {:ok, _record} = SPQueue.dequeue(pq, id: id + i) end)
    {:error, :empty} = SPQueue.dequeue(pq)
    {:error, :empty} = SPQueue.head(pq)
    stop(pq)
  end

  test "to list conversion", %{line: line} = _context do
    pq = start_unique(line)
    1..10 |> Enum.each(fn i -> SPQueue.enqueue(pq, %{"i" => i}) end)
    assert(SPQueue.to_list(pq) == 1..10 |> Enum.map(fn i -> %{"i" => i} end))
    11..50 |> Enum.each(fn i -> SPQueue.enqueue(pq, %{"i" => i}) end)
    assert(SPQueue.to_list(pq) == 1..50 |> Enum.map(fn i -> %{"i" => i} end))
    stop(pq)
  end

  test "delegate receives dequeued", %{line: line} = _context do
    pq = start_unique(line, delegate: self())
    SPQueue.enqueue(pq, %{"test" => 123})
    assert_receive :enqueued
    stop(pq)
  end

  test "simple worker", %{line: line} = _context do
    queue_name = String.to_atom(unique_name_for_test(line))
    worker_name = String.to_atom("test-worker-#{line}")
    test_process = self()

    {:ok, worker} =
      SPQueueWorker.start(
        queue_name: queue_name,
        name: worker_name,
        handler_function: fn msg ->
          send(test_process, msg)
          :ack
        end
      )

    pq = start_unique(line, delegate: worker)
    SPQueue.enqueue(pq, %{"test" => 123})
    assert_receive %{"test" => 123}
    stop(pq)
    SPQueueWorker.stop(worker)
  end

  test "drain queue by worker", %{line: line} = _context do
    queue_name = String.to_atom(unique_name_for_test(line))
    worker_name = String.to_atom("test-worker-#{line}")
    test_process = self()
    limit = 1000

    {:ok, worker} =
      SPQueueWorker.start(
        queue_name: queue_name,
        name: worker_name,
        handler_function: fn %{"i" => i} = _msg ->
          if i == limit, do: send(test_process, :done)
          :ack
        end
      )

    pq = start_unique(line, delegate: worker)
    1..limit |> Enum.each(fn i -> SPQueue.enqueue_wait!(pq, %{"i" => i}) end)
    assert_receive :done
    assert(SPQueue.empty?(pq))
    stop(pq)
    SPQueueWorker.stop(worker)
  end

  # support

  defp unique_name_for_test(id), do: "test-pq-#{id}"

  defp start_unique(id, opts \\ [clean: true]) do
    name = unique_name_for_test(id)
    Keyword.get(opts, :clean, true) && File.rm_rf!(name)
    {:ok, pq} = SPQueue.start([name: String.to_atom(name)] ++ opts)
    pq
  end

  defp stop(pq, opts \\ [clean: true]) do
    Keyword.get(opts, :clean, true) && queue_base_dir(pq) |> File.rm_rf!()
    SPQueue.stop(pq)
  end

  defp queue_base_dir(pq) do
    SPQueue.info(pq)[:queue_base_dir]
  end
end
