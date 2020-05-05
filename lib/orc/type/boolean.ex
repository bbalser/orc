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
      Orc.Stream.encode(t, list)
      |> List.wrap()
    end

    def values(t, streams) do
      data_stream = Keyword.fetch!(streams, :DATA)

      Orc.Stream.decode(t, data_stream)
    end
  end
end
