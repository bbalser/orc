defmodule Orc.Chunk do
  @type t :: %__MODULE__{
          type: Orc.Type.t(),
          binary: binary(),
          stats: Orc.Proto.ColumnStatistics.t()
        }

  @enforce_keys [:type, :binary, :stats]
  defstruct [:type, :binary, :stats]

  def new(attrs) do
    struct!(__MODULE__, attrs)
  end

  def binary_size(%__MODULE__{binary: binary}) do
    byte_size(binary)
  end

  def binary(%__MODULE__{binary: binary}) do
    binary
  end
end

defprotocol Orc.Chunk.Encoder do
  @spec encode(t, list) :: Orc.Chunk.t()
  def encode(t, list)

  @spec chunk_size(t) :: pos_integer()
  def chunk_size(t)
end

defprotocol Orc.Chunk.Decoder do
  @spec decode(t, binary) :: {list, binary}
  def decode(t, binary)
end
