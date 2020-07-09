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

    def index(_t, [stream | _] = streams) do
      streams_by_kind =
        Enum.reduce(streams, %{}, fn stream, acc -> Map.put(acc, stream.kind, stream) end)

      case Map.has_key?(streams_by_kind, :PRESENT) do
        false ->
          Orc.RowIndex.create(stream, fn c, d, l -> [c, d, leftover_bytes(l), 0] end)

        true ->
          present_index =
            Map.get(streams_by_kind, :PRESENT)
            |> Orc.RowIndex.create(fn c, d, l -> [c, d, leftover_bytes(l), 0] end)

          data_index =
            Map.get(streams_by_kind, :DATA)
            |> Orc.RowIndex.create(fn c, d, l -> [c, d, leftover_bytes(l), 0] end)

          entries =
          Enum.zip(data_index.entry, present_index.entry)
          |> Enum.map(fn {de, pe} ->
            %{de | positions: pe.positions ++ de.positions}
          end)

          Orc.Proto.RowIndex.new(entry: entries)
      end
    end

    defp leftover_bytes(length) do
      (length / 8)
      |> Float.ceil()
      |> round()
    end
  end
end
