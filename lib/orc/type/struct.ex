defmodule Orc.Type.Struct do
  defstruct name: nil, children: []

  def new(attrs) do
    struct!(__MODULE__, attrs)
  end

  defimpl Orc.Type do
    def to_list(%{children: children}, column \\ 0) do
      {child_index_types, _} =
        Enum.map_reduce(children, column + 1, fn child, index ->
          sub_types = Orc.Type.to_list(child, index)

          {{index, sub_types}, index + length(sub_types)}
        end)

      subtypes = Enum.map(child_index_types, fn {index, _} -> index end)
      child_types = Enum.map(child_index_types, fn {_, types} -> types end) |> List.flatten()

      type =
        Orc.Proto.Type.new(
          kind: :STRUCT,
          fieldNames: Enum.map(children, &Map.get(&1, :name)),
          subtypes: subtypes
        )

      [type | child_types]
      |> List.flatten()
    end

    def column_encoding(%{children: children}) do
      [
        Orc.Proto.ColumnEncoding.new(kind: :DIRECT)
        | Enum.map(children, &Orc.Type.column_encoding/1)
      ]
      |> List.flatten()
    end

    def streams(%{children: children}, list) do
      Enum.flat_map(children, fn child ->
        child_data = Enum.map(list, &Map.get(&1, child.name))
        Orc.Type.streams(child, child_data)
      end)
    end
  end
end
