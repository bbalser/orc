defmodule Orc.Stream do
  alias Orc.CompressedBlock

  @type t :: %__MODULE__{
          type: Orc.Type.t(),
          kind: :DATA | :LENGTH | :PRESENT,
          blocks: [Orc.CompressedBlock.t()]
        }

  defstruct type: nil, kind: nil, blocks: []

  def new(attrs) do
    struct!(__MODULE__, attrs)
  end

  def binary(%__MODULE__{blocks: blocks}) do
    blocks
    |> Enum.map(&Map.get(&1, :binary))
    |> Enum.join()
  end

  def encode(t, list) do
    blocks =
      list
      |> Enum.chunk_every(Orc.Chunk.Encoder.chunk_size(t))
      |> Enum.map(&Orc.Chunk.Encoder.encode(t, &1))
      |> Enum.chunk_while(
        CompressedBlock.new(type: t),
        &add_chunk_to_block(t, &1, &2),
        &{:cont, &1, :ok}
      )
      |> Enum.map(&CompressedBlock.compress/1)

    Orc.Stream.new(
      type: t,
      kind: :DATA,
      blocks: blocks
    )
  end

  def decode(t, binary) do
    binary
    |> Orc.Compression.decompress()
    |> Stream.unfold(fn
      <<>> -> nil
      acc -> Orc.Chunk.Decoder.decode(t, acc)
    end)
    |> Enum.to_list()
    |> List.flatten()
  end

  defp add_chunk_to_block(type, chunk, block) do
    case CompressedBlock.fit?(block, chunk) do
      true ->
        {:cont, CompressedBlock.add(block, chunk)}

      false ->
        new_block = CompressedBlock.new(type: type) |> CompressedBlock.add(chunk)
        {:cont, block, new_block}
    end
  end
end
