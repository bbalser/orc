defprotocol Orc.Chunk.Encoder do
  @spec encode(t, list) :: Orc.Chunk.t()
  def encode(t, list)
end

defprotocol Orc.Chunk.Decoder do
  @spec decode(t, binary) :: {list, binary}
  def decode(t, binary)
end

defmodule Orc.Chunk do

  @type t :: %__MODULE__{
    type: Orc.Type.t(),
    binary: binary(),
    stats: Orc.Proto.ColumnStatistics.t()
  }

  @enforce_keys [:type, :binary, :stats]
  defstruct [:type, :binary, :stats]
end
