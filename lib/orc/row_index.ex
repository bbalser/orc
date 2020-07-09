defmodule Orc.RowIndex do
  alias Orc.Chunk
  alias Orc.CompressedBlock

  @row_size 10_000

  @spec create(
          Orc.Stream.t(),
          (non_neg_integer(), non_neg_integer(), non_neg_integer() -> [integer()])
        ) :: Orc.Proto.RowIndex.t()
  def create(stream, position_fun \\ &create_position/3) do
    indexed_chunks = index_chunks(stream)
    entries = create_entries(indexed_chunks, position_fun)

    Orc.Proto.RowIndex.new(entry: entries)
  end

  defp index_chunks(stream) do
    {indexed_chunks, _} =
    Enum.flat_map_reduce(stream.blocks, 0, fn block, block_address ->
      {chunks, _} =
        Enum.map_reduce(block.chunks, 0, fn chunk, chunk_address ->
          {{block_address, chunk_address, chunk}, chunk_address + Chunk.binary_size(chunk)}
        end)

      {chunks, block_address + CompressedBlock.binary_size(block)}
    end)

    indexed_chunks
  end

  defp create_entries(chunks, position_fun) do
    Enum.chunk_while(
      chunks,
      %{count: 0, entry: new_entry(position_fun.(0, 0, 0))},
      fn {block_address, chunk_address, chunk}, acc ->
        case acc.count + Chunk.size(chunk) > @row_size do
          false ->
            {:cont,
             Map.update!(acc, :count, &(&1 + Chunk.size(chunk)))
             |> Map.update!(:entry, &merge_into(&1, Chunk.stats(chunk), chunk.type))}

          true ->
            leftover = @row_size - acc.count
            positions = position_fun.(block_address, chunk_address, leftover)
            {included, excluded} = leftover_values(chunk, leftover)

            entry =
              merge_into(acc.entry, Orc.Statistics.calculate(chunk.type, included) |> hd(), chunk.type)

            new_entry = new_entry(positions, Orc.Statistics.calculate(chunk.type, excluded) |> hd())

            new_acc =
              Map.put(acc, :count, Chunk.size(chunk) - leftover)
              |> Map.put(:entry, new_entry)

            {:cont, entry, new_acc}
        end
      end,
      fn acc -> {:cont, acc.entry, acc} end
    )
  end

  defp leftover_values(chunk, leftover) do
    {values, <<>>} = Orc.Chunk.Decoder.decode(chunk.type, chunk.binary)
    Enum.split(values, leftover)
  end

  defp new_entry(positions, stats \\ nil) do
    Orc.Proto.RowIndexEntry.new(positions: positions, statistics: stats)
  end

  defp merge_into(%Orc.Proto.RowIndexEntry{statistics: nil} = entry, stats, _type) do
    %{entry | statistics: stats}
  end

  defp merge_into(entry, stats, type) do
    %{entry | statistics: Orc.Statistics.merge(type, entry.statistics, stats)}
  end

  defp create_position(compressed, decompressed, leftover) do
    [compressed, decompressed, leftover]
  end
end
