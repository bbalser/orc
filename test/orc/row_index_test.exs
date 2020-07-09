defmodule Orc.RowIndexTest do
  use ExUnit.Case
  import TestData

  alias Orc.Chunk
  alias Orc.CompressedBlock

  defmodule TestType do
    defstruct []

    defimpl Orc.Statistics do
      def calculate(_t, list) do
        Orc.Proto.ColumnStatistics.new(numberOfValues: length(list))
      end

      def merge(_t, stats1, stats2) do
        %{stats1 | numberOfValues: stats1.numberOfValues + stats2.numberOfValues}
      end
    end

    defimpl Orc.Chunk.Decoder do
      def decode(_t, binary) do
        {String.codepoints(binary), <<>>}
      end
    end
  end

  describe "index for less than 10_000 rows" do
    setup do
      block = block(chunks: [chunk(count: 9_999)])
      stream = stream(blocks: [block])
      index = Orc.RowIndex.create(stream)

      [index: index]
    end

    test "will create default position", %{index: index} do
      positions =
        Map.get(index, :entry)
        |> Enum.map(&Map.get(&1, :positions))

      assert positions == [[0, 0, 0]]
    end

    test "will have stats", %{index: index} do
      [stats] =
        Map.get(index, :entry)
        |> Enum.map(&Map.get(&1, :statistics))

      assert 9_999 == stats.numberOfValues
    end
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

  describe "create multiple entries for over 10_000 values" do
    setup do
      chunk1 = chunk(count: 9_999)
      chunk2 = chunk(count: 10)

      block = block(chunks: [chunk1, chunk2])
      stream = stream(blocks: [block])

      index = Orc.RowIndex.create(stream)

      [index: index, chunk1: chunk1]
    end

    test "multiple positions", %{index: index, chunk1: chunk1} do
      positions =
        Map.get(index, :entry)
        |> Enum.map(&Map.get(&1, :positions))

      assert positions == [
        [0, 0, 0],
        [0, Chunk.binary_size(chunk1), 1]
      ]
    end

    test "stats", %{index: index} do
      [stats1, stats2] =
         Map.get(index, :entry)
        |> Enum.map(&Map.get(&1, :statistics))

      assert 10_000 == stats1.numberOfValues
      assert 9 == stats2.numberOfValues
    end
  end

  describe "multiple compressed blocks" do
    setup do
      chunk1 = chunk(count: 9_999)
      chunk2 = chunk(count: 10)
      chunk3 = chunk(count: 12_000)
      chunk4 = chunk(count: 1_000)

      block1 = block(chunks: [chunk1, chunk2])
      block2 = block(chunks: [chunk3, chunk4])

      stream = stream(blocks: [block1, block2])

      index = Orc.RowIndex.create(stream)

      [index: index, chunk1: chunk1, block1: block1]
    end

    test "are referenced in positions", %{index: index, chunk1: chunk1, block1: block1} do
      positions =
        Map.get(index, :entry)
        |> Enum.map(&Map.get(&1, :positions))

      assert positions == [
        [0, 0, 0],
        [0, Chunk.binary_size(chunk1), 1],
        [CompressedBlock.binary_size(block1), 0, 9_991]
      ]
    end

    test "stats are correct acccross blocks", %{index: index} do
      [stats1, stats2, stats3] =
        Map.get(index, :entry)
        |> Enum.map(&Map.get(&1, :statistics))

      assert 10_000 == stats1.numberOfValues
      assert 10_000 == stats2.numberOfValues
      assert 3_009 == stats3.numberOfValues
    end
  end

  defp chunk(opts) do
    count = Keyword.fetch!(opts, :count)

    Chunk.new(
      type: %TestType{},
      binary: random_string(count),
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
