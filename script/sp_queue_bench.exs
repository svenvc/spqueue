count = System.get_env("COUNT", "1000") |> String.to_integer()

IO.puts("Persistent Queue Benchmark\nSending and consuming #{count} messages")

queue_name = :pq
worker_name = :pqw

File.rm_rf!("pq")

parent = self()

{:ok, worker} =
  SPQueueWorker.start(
    queue_name: queue_name,
    name: worker_name,
    handler_function: fn msg ->
      if msg["index"] == count - 1 do
        send(parent, {:done, msg})
      end

      :ack
    end
  )

{:ok, queue} =
  SPQueue.start(
    name: queue_name,
    delegate: worker
  )

defmodule Util do
  def enqueue(q, m, t \\ 100) do
    if !SPQueue.enqueue(q, m) do
      if t >= 0 do
        Process.sleep(1)
        enqueue(q, m, t - 1)
      else
        raise "timed_out"
      end
    end
  end
end

{us, _} =
  :timer.tc(fn ->
    0..(count - 1)
    |> Enum.each(fn i ->
      Util.enqueue(
        queue,
        %{
          "index" => i,
          "info" => "this is a benchmark",
          "test" => true,
          "xy" => [1.5, -2.5],
          "q" => :rand.uniform()
        }
      )
    end)
  end)

IO.puts("#{count} messages took #{us / 1_000_000} s")
IO.puts("#{Float.round(us / count, 2)} μs/msg")
IO.puts("#{Float.round(count / us * 1_000_000, 2)} msg/s")

receive do
  {:done, last_msg} -> IO.inspect(last_msg, label: "Done, last msg")
end

IO.inspect(SPQueue.empty?(queue), label: "Queue empty ?")

SPQueueWorker.stop(worker)
SPQueue.stop(queue)

File.rm_rf!("pq")
