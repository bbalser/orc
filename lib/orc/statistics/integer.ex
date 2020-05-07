defimpl Orc.Statistics, for: Orc.Type.Integer do
  def calculate(_t, list) do
    [head | tail] = Enum.reject(list, &is_nil/1)

    {min, max, sum} =
      Enum.reduce(tail, {head, head, head}, fn i, {min, max, sum} ->
        {min(min, i), max(max, i), sum + i}
      end)

    nils? = Enum.any?(list, &is_nil/1)

    Orc.Proto.ColumnStatistics.new(
      numberOfValues: length(list),
      hasNull: nils?,
      intStatistics:
        Orc.Proto.IntegerStatistics.new(
          minimum: min,
          maximum: max,
          sum: sum
        )
    )
    |> List.wrap()
  end

  def merge(_t, stats1, stats2) do
    %{
      stats1
      | numberOfValues: stats1.numberOfValues + stats2.numberOfValues,
        hasNull: stats1.hasNull || stats2.hasNull,
        intStatistics: merge_int_stats(stats1.intStatistics, stats2.intStatistics)
    }
  end

  defp merge_int_stats(stats1, stats2) do
    %{
      stats1
      | minimum: min(stats1.minimum, stats2.minimum),
        maximum: max(stats1.maximum, stats2.maximum),
        sum: stats1.sum + stats2.sum
    }
  end
end
