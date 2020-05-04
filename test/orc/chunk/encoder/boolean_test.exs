defmodule Orc.Chunk.Encoder.BooleanTest do
  use ExUnit.Case

  setup do
    input = [true] ++ Enum.map(1..7, fn _ -> false end)

    chunk = Orc.Chunk.Encoder.encode(Orc.boolean(), input)

    [chunk: chunk]
  end

  test "1 true followed by 7 false", %{chunk: chunk} do
    assert Base.encode16(chunk.binary, case: :lower) == "ff80"
  end

  test "calculates stats for booleans", %{chunk: chunk} do
    assert 8 == chunk.stats.numberOfValues
    assert [1] == chunk.stats.bucketStatistics.count
  end
end
