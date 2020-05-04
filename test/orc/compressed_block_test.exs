defmodule Orc.CompressedBlockTest do
  use ExUnit.Case
  import TestData

  alias Orc.Chunk
  alias Orc.CompressedBlock

  defmodule TestType do
    defstruct []
  end

  test "can add chunks to block" do
    chunk =
      Chunk.new(
        type: %TestType{},
        binary: random_string(100),
        stats: Orc.Proto.ColumnStatistics.new(numberOfValues: 12)
      )

    block = CompressedBlock.new(type: chunk.type) |> CompressedBlock.add(chunk)

    assert [chunk] == block.chunks
  end

  test "can add chunks if they fit in max_size" do
    chunk =
      Chunk.new(
        type: %TestType{},
        binary: random_string(100),
        stats: Orc.Proto.ColumnStatistics.new(numberOfValues: 10)
      )

    block = CompressedBlock.new(max_size: 1_000)

    block =
      Enum.reduce(1..10, block, fn _, acc ->
        CompressedBlock.add(acc, chunk)
      end)

    assert false == CompressedBlock.fit?(block, chunk)
  end

  test "compress will create compressed binary" do
    chunk =
      Chunk.new(
        type: %TestType{},
        binary: random_string(100),
        stats: Orc.Proto.ColumnStatistics.new(numberOfValues: 10)
      )

    block = CompressedBlock.new(max_size: 1_000)

    block =
      Enum.reduce(1..10, block, fn _, acc ->
        CompressedBlock.add(acc, chunk)
      end)
      |> CompressedBlock.compress()

    expected = Enum.reduce(1..10, <<>>, fn _, acc ->
      acc <> chunk.binary
    end)
    |> Orc.Compression.compress()

    assert expected == block.binary
  end
end
