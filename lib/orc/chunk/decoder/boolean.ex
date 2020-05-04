defimpl Orc.Chunk.Decoder, for: Orc.Type.Boolean do
  def decode(_t, binary) do
    {data, remaining} = decode_bytes(binary)

    booleans =
      Stream.unfold(data, fn
        <<>> -> nil
        <<1::size(1), rest::bitstring>> -> {true, rest}
        <<0::size(1), rest::bitstring>> -> {false, rest}
      end)
      |> Enum.to_list()

    {booleans, remaining}
  end

  defp decode_bytes(<<length::integer-signed-size(8), rest::binary>>) when length < 0 do
    length = - length
    <<data::binary-size(length), remaining::binary>> = rest
    {data, remaining}
  end

  defp decode_bytes(<<length::integer-signed-size(8), repeated_byte::binary-size(1), remaining::binary>>) do
    data = Enum.reduce(1..(length + 3), <<>>, fn _, acc -> acc <> repeated_byte end)

    {data, remaining}
  end
end
