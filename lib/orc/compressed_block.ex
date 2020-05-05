defmodule Orc.CompressedBlock do
  alias Orc.Chunk

  @max_size 262_144

  @type t :: %__MODULE__{
          type: Orc.Type.t(),
          estimated_size: pos_integer(),
          chunks: [Orc.Chunk.t()],
          binary: Orc.compressed_binary(),
          max_size: pos_integer()
        }

  defstruct type: nil, estimated_size: 0, chunks: [], binary: nil, stats: nil, max_size: @max_size

  def new(attrs) do
    struct!(__MODULE__, attrs)
  end

  def binary_size(%__MODULE__{binary: binary}) do
    byte_size(binary)
  end

  def fit?(%__MODULE__{} = block, %Chunk{} = chunk) do
    Chunk.binary_size(chunk) + block.estimated_size <= block.max_size
  end

  def add(%__MODULE__{} = block, %Chunk{} = chunk) do
    %{
      block
      | chunks: block.chunks ++ [chunk],
        estimated_size: block.estimated_size + Chunk.binary_size(chunk)
    }
  end

  def compress(%__MODULE__{} = block) do
    compressed_binary =
      block.chunks
      |> Enum.map(&Chunk.binary/1)
      |> Enum.join()
      |> Orc.Compression.compress()

    %{block | binary: compressed_binary}
  end
end
