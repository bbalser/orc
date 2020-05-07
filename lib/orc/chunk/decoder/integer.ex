defimpl Orc.Chunk.Decoder, for: Orc.Type.Integer do
  def decode(t, binary) do
    do_decode(binary, t.signed?)
  end

  defp do_decode(<<1::size(2), _::bitstring>> = binary, signed?) do
    Orc.Chunk.Decoder.Integer.Direct.decode(binary, signed?)
  end

  defp do_decode(<<3::size(2), _::bitstring>> = binary, signed?) do
    Orc.Chunk.Decoder.Integer.Delta.decode(binary, signed?)
  end
end
