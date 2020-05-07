defmodule Orc.Type.Integer do
  @type t :: %__MODULE__{
          name: String.t(),
          signed?: boolean()
        }

  defstruct name: nil, signed?: false

  def new(attrs) do
    struct!(__MODULE__, attrs)
  end

  defimpl Orc.Type do
    def to_list(_t, _column \\ 0) do
      [Orc.Proto.Type.new(kind: :INT)]
    end

    def column_encoding(_t) do
      [Orc.Proto.ColumnEncoding.new(kind: :DIRECT_V2)]
    end

    def streams(t, list) do
      integers = Enum.reject(list, &is_nil/1)
      data_stream = Orc.Stream.encode(t, integers)

      Orc.Stream.encode_presence(list) ++ [data_stream]
    end

    def values(t, streams) do
      data_stream = Keyword.fetch!(streams, :DATA)
      integers = Orc.Stream.decode(t, data_stream)

      Orc.Stream.decode_presence(Keyword.get(streams, :PRESENT), integers)
    end
  end
end
