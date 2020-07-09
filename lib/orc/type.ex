defprotocol Orc.Type do
  @spec to_list(t) :: [Orc.Proto.Type.t()]
  def to_list(t)

  @spec to_list(t, non_neg_integer()) :: [Orc.Proto.Type.t()]
  def to_list(t, column)

  @spec column_encoding(t) :: [Orc.Proto.ColumnEncoding.t()]
  def column_encoding(t)

  @spec streams(t, list) :: [Orc.Stream.t()]
  def streams(t, list)

  @spec index(t, [Orc.Stream.t()]) :: Orc.Proto.RowIndex.t()
  def index(t, streams)
end
