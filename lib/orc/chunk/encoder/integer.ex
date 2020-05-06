defimpl Orc.Chunk.Encoder, for: Orc.Type.Integer do

  def encode(t, list) do
    binary = Orc.Chunk.Encoder.Integer.Direct.encode(list, t.signed?)

    Orc.Chunk.new(
      type: t,
      binary: binary,
      stats: Orc.Statistics.calculate(t, list)
    )
  end

  def chunk_size(_t), do: 512

end
