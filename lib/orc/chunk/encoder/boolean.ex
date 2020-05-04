defimpl Orc.Chunk.Encoder, for: Orc.Type.Boolean do
  def encode(_t, list) do
    bits =
      Enum.reduce(list, <<>>, fn

        true, acc -> <<acc::bitstring, 1::size(1)>>
      false, acc -> <<acc::bitstring, 0::size(1)>>
      end)

    binary = Orc.Helper.pad_to_binary(bits)
    length = -byte_size(binary)

    <<length::integer-signed-size(8)>> <> binary
  end

  def chunk_size(_t), do: 1024
end
