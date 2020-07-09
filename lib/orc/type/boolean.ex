defmodule Orc.Type.Boolean do
  defstruct [:name]

  def new(attrs) do
    struct!(__MODULE__, attrs)
  end

  defimpl Orc.Type do
    def to_list(_t, _column \\ 0) do
      [Orc.Proto.Type.new(kind: :BOOLEAN)]
    end

    def column_encoding(_t) do
      [Orc.Proto.ColumnEncoding.new(kind: :DIRECT)]
    end

    def streams(t, list) do
      booleans = Enum.reject(list, &is_nil/1)
      data_stream = Orc.Stream.encode(t, booleans)

      Orc.Stream.encode_presence(list) ++ [data_stream]
    end

    def values(t, streams) do
      data_stream = Keyword.fetch!(streams, :DATA)
      booleans = Orc.Stream.decode(t, data_stream)

      Keyword.get(streams, :PRESENT)
      |> Orc.Stream.decode_presence(booleans)
    end
  end
end
