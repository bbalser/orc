defmodule Orc.RowIndexTest do
  use ExUnit.Case
  import TestData

  alias Orc.Chunk
  alias Orc.CompressedBlock

  defmodule TestType do
    defstruct []
  end

  test "will create default position when number of values less than 10_000" do
    block = block(chunks: [chunk(count: 9_999)])
    stream = stream(blocks: [block])

    positions =
      Orc.RowIndex.create(stream)
      |> Map.get(:entry)
      |> Enum.map(&Map.get(&1, :positions))

    assert positions == [[0, 0, 0]]
  end

  test "will use custom position function to create position array" do
    block = block(chunks: [chunk(count: 9_999)])
    stream = stream(blocks: [block])

    positions =
      Orc.RowIndex.create(stream, fn c, d, l -> [c, d, l, 0] end)
      |> Map.get(:entry)
      |> Enum.map(&Map.get(&1, :positions))

    assert positions == [[0, 0, 0, 0]]
  end

  test "creates positions for 2 chunk over 10_000 values" do
    chunk1 = chunk(count: 9_999)
    chunk2 = chunk(count: 10)

    block = block(chunks: [chunk1, chunk2])
    stream = stream(blocks: [block])

    positions =
      Orc.RowIndex.create(stream)
      |> Map.get(:entry)
      |> Enum.map(&Map.get(&1, :positions))

    assert positions == [
             [0, 0, 0],
             [0, Chunk.binary_size(chunk1), 1]
           ]
  end

  test "multiple compressed blocks" do
    chunk1 = chunk(count: 9_999)
    chunk2 = chunk(count: 10)
    chunk3 = chunk(count: 12_000)
    chunk4 = chunk(count: 1_000)

    block1 = block(chunks: [chunk1, chunk2])
    block2 = block(chunks: [chunk3, chunk4])

    stream = stream(blocks: [block1, block2])

    positions =
      Orc.RowIndex.create(stream)
      |> Map.get(:entry)
      |> Enum.map(&Map.get(&1, :positions))

    assert positions == [
             [0, 0, 0],
             [0, Chunk.binary_size(chunk1), 1],
             [CompressedBlock.binary_size(block1), 0, 9_991]
           ]
  end

  defp chunk(opts) do
    count = Keyword.fetch!(opts, :count)

    Chunk.new(
      type: %TestType{},
      binary: random_string(1_000),
      stats: Orc.Proto.ColumnStatistics.new(numberOfValues: count)
    )
  end

  defp block(opts) do
    chunks = Keyword.fetch!(opts, :chunks)

    Enum.reduce(chunks, CompressedBlock.new(type: %TestType{}), &CompressedBlock.add(&2, &1))
    |> CompressedBlock.compress()
  end

  defp stream(opts) do
    kind = Keyword.get(opts, :kind, :DATA)
    blocks = Keyword.get(opts, :blocks)

    Orc.Stream.new(
      type: %TestType{},
      kind: kind,
      blocks: blocks
    )
  end

end
