defmodule Orc.RowIndex do
  alias Orc.Chunk
  alias Orc.CompressedBlock

  @row_size 10_000

  @spec create(
          Orc.Stream.t(),
          (non_neg_integer(), non_neg_integer(), non_neg_integer() -> [integer()])
        ) :: Orc.Proto.RowIndex.t()
  def create(stream, position_fun \\ &create_position/3) do
    {chunks, _} =
      Enum.flat_map_reduce(stream.blocks, 0, fn block, block_address ->
        {chunks, _} =
          Enum.map_reduce(block.chunks, 0, fn chunk, chunk_address ->
            {{block_address, chunk_address, chunk}, chunk_address + Chunk.binary_size(chunk)}
          end)

        {chunks, block_address + CompressedBlock.binary_size(block)}
      end)

    entries =
      ([position_fun.(0, 0, 0)] ++ determine_positions(chunks, position_fun))
      |> Enum.map(fn position -> Orc.Proto.RowIndexEntry.new(positions: position) end)

    Orc.Proto.RowIndex.new(entry: entries)
  end

  defp determine_positions(chunks, position_fun) do
    Enum.chunk_while(
      chunks,
      %{count: 0},
      fn {block_address, chunk_address, chunk}, acc ->
        case acc.count + Chunk.size(chunk) > @row_size do
          false ->
            {:cont, Map.update!(acc, :count, &(&1 + Chunk.size(chunk)))}

          true ->
            leftover = @row_size - acc.count
            position = position_fun.(block_address, chunk_address, leftover)

            new_acc = Map.put(acc, :count, Chunk.size(chunk) - leftover)

            {:cont, position, new_acc}
        end
      end,
      fn acc -> {:cont, acc} end
    )
  end

  defp create_position(compressed, decompressed, leftover) do
    [compressed, decompressed, leftover]
  end
end
