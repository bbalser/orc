defimpl Orc.Chunk.Decoder, for: Orc.Type.Boolean do
  def decode(_t, binary) do
    {data, remaining} = Orc.Chunk.Decoder.decode(Orc.byte(), binary)

    booleans =
      Stream.unfold(data, fn
        <<>> -> nil
        <<1::size(1), rest::bitstring>> -> {true, rest}
        <<0::size(1), rest::bitstring>> -> {false, rest}
      end)
      |> Enum.to_list()

    {booleans, remaining}
  end
end
