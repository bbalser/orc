defimpl Orc.Chunk.Encoder, for: Orc.Type.Byte do
  def encode(_t, bytes) do
    length = - byte_size(bytes)
    <<length::integer-signed-size(8)>> <> bytes
  end
end
