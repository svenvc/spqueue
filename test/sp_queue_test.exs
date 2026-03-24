defmodule SPQueue.Test do
  use ExUnit.Case, async: true
  doctest SPQueue

  test "initial state", %{line: line} = _context do
    pq = start_unique(line)
    assert(SPQueue.head(pq) == nil)
    assert(SPQueue.count(pq) == 0)
    assert(SPQueue.empty?(pq))
    stop(pq)
  end

  test "simple enqueue/dequeue", %{line: line} = _context do
    pq = start_unique(line)
    1..3 |> Enum.each(fn n -> SPQueue.enqueue(pq, %{"n" => n}) end)
    assert(SPQueue.count(pq) == 3)
    assert(SPQueue.head(pq) == %{"n" => 1})
    1..3 |> Enum.each(fn n -> assert(SPQueue.dequeue(pq) |> Map.get("n") == n) end)
    assert(SPQueue.empty?(pq))
    stop(pq)
  end

  test "load state", %{line: line} = _context do
    pq = start_unique(line)
    1..3 |> Enum.each(fn n -> SPQueue.enqueue(pq, %{"n" => n}) end)
    SPQueue.dequeue(pq)
    assert(SPQueue.count(pq) == 2)
    assert(SPQueue.head(pq) == %{"n" => 2})
    stop(pq, clean: false)
    pq = start_unique(line, clean: false)
    assert(SPQueue.count(pq) == 2)
    assert(SPQueue.head(pq) == %{"n" => 2})
    stop(pq)
  end

  test "multiple segments 32/16", %{line: line} = _context do
    pq = start_unique(line)
    1..32 |> Enum.each(fn n -> SPQueue.enqueue(pq, %{"n" => n}) end)
    1..16 |> Enum.each(fn n -> assert(SPQueue.dequeue(pq) |> Map.get("n") == n) end)
    assert(SPQueue.head(pq) == %{"n" => 17})
    assert(SPQueue.count(pq) == 16)
    stop(pq)
  end

  test "multiple segments 40", %{line: line} = _context do
    pq = start_unique(line)
    1..40 |> Enum.each(fn n -> SPQueue.enqueue(pq, %{"n" => n}) end)
    assert(SPQueue.count(pq) == 40)
    1..40 |> Enum.each(fn n -> assert(SPQueue.dequeue(pq) == %{"n" => n}) end)
    assert(SPQueue.empty?(pq))
    stop(pq)
  end

  test "load state multiple segments", %{line: line} = _context do
    pq = start_unique(line)
    1..32 |> Enum.each(fn n -> SPQueue.enqueue(pq, %{"n" => n}) end)
    1..16 |> Enum.each(fn n -> assert(SPQueue.dequeue(pq) |> Map.get("n") == n) end)
    assert(SPQueue.head(pq) == %{"n" => 17})
    assert(SPQueue.count(pq) == 16)
    stop(pq, clean: false)
    pq = start_unique(line, clean: false)
    assert(SPQueue.head(pq) == %{"n" => 17})
    assert(SPQueue.count(pq) == 16)
    stop(pq)
  end

  test "full", %{line: line} = _context do
    pq = start_unique(line)
    1..50 |> Enum.each(fn n -> assert(SPQueue.enqueue(pq, %{"n" => n})) end)
    refute(SPQueue.empty?(pq))
    assert(SPQueue.count(pq) == 50)
    refute(SPQueue.enqueue(pq, %{"n" => 51}))
    stop(pq)
  end

  test "gc", %{line: line} = _context do
    pq = start_unique(line)

    0..4
    |> Enum.each(fn round ->
      1..40 |> Enum.each(fn i -> assert(SPQueue.enqueue(pq, %{"n" => round * 40 + i})) end)
      assert(SPQueue.count(pq) == 40)
      1..40 |> Enum.each(fn i -> assert(SPQueue.dequeue(pq) == %{"n" => round * 40 + i}) end)
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
      |> Enum.each(fn n -> assert(SPQueue.enqueue(pq, %{"n" => n})) end)

      total_dequeued_next = total_dequeued + to_dequeue

      total_dequeued..(total_dequeued_next - 1)
      |> Enum.each(fn n -> assert(SPQueue.dequeue(pq) == %{"n" => n}) end)

      {total_enqueued_next, total_dequeued_next}
    end)

    assert(Enum.empty?(queue_base_dir(pq) |> File.ls!()))
    stop(pq)
  end

  test "dequeue with id", %{line: line} = _context do
    pq = start_unique(line)
    100..105 |> Enum.each(fn n -> {:ok, _record} = SPQueue.enqueue_r(pq, %{"code" => n}) end)
    {:ok, _record} = SPQueue.dequeue_r(pq)
    {:ok, %{"id" => id} = _record} = SPQueue.head_r(pq)
    {:ok, _record} = SPQueue.dequeue_r(pq, id: id)
    {:error, :mismatch} = SPQueue.dequeue_r(pq, id: id)
    1..4 |> Enum.each(fn i -> {:ok, _record} = SPQueue.dequeue_r(pq, id: id + i) end)
    {:error, :empty} = SPQueue.dequeue_r(pq)
    {:error, :empty} = SPQueue.head_r(pq)
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

    {:ok, worker} =
      SPQueueWorker.start(
        queue_name: queue_name,
        name: worker_name,
        handler_function: fn %{"i" => i} = _msg ->
          if i == 1000, do: send(test_process, :done)
          :ack
        end)

    pq = start_unique(line, delegate: worker)
    1..1000 |> Enum.each(fn i -> SPQueue.enqueue(pq, %{"i" => i}) end)
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
