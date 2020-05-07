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

  def encode(t, list, kind \\ :DATA) do
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
      kind: kind,
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

  @spec encode_presence(list) :: [] | [Orc.Stream.t()]
  def encode_presence(list) do
    case Enum.any?(list, &is_nil/1) do
      true ->
        presence = Enum.map(list, fn i -> i != nil end)

        encode(Orc.boolean(), presence, :PRESENT)
        |> List.wrap()

      false ->
        []
    end
  end

  @spec decode_presence(Orc.compressed_binary() | nil, list) :: list
  def decode_presence(nil, values), do: values

  def decode_presence(presence_binary, values) do
    presence = decode(Orc.boolean(), presence_binary)

    {decoded_values, _} =
      Enum.map_reduce(presence, values, fn
        true, [head | tail] -> {head, tail}
        false, integers -> {nil, integers}
      end)

    decoded_values
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
