defmodule Orc.Chunk.Decoder.ByteTest do
  use ExUnit.Case

  test "literal decoding" do
    bytes = Base.decode16!("fe4445", case: :lower)

    {decoded, <<67, 89>>} = Orc.Chunk.Decoder.decode(Orc.byte(), bytes <> <<67, 89>>)

    assert decoded == binary_part(bytes, 1, 2)
  end

  test "run length decoding" do
    bytes = Base.decode16!("6100", case: :lower)

    {decoded, <<89, 67>>} = Orc.Chunk.Decoder.decode(Orc.byte(), bytes <> <<89, 67>>)

    assert Enum.reduce(1..100, <<>>, fn _, acc -> acc <> <<0>> end) == decoded
  end
end
