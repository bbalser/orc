defmodule Orc.Chunk.Decoder.Integer.DeltaTest do
  use ExUnit.Case

  test "decodes data stream into integers" do
    integers = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]

    bytes = Orc.Chunk.Encoder.Integer.Delta.encode(integers, false)

    assert {integers, <<>>} == Orc.Chunk.Decoder.Integer.Delta.decode(bytes, false)
  end

  test "decodes data stream into signed integers" do
    integers = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]

    bytes = Orc.Chunk.Encoder.Integer.Delta.encode(integers, true)

    assert {integers, <<>>} == Orc.Chunk.Decoder.Integer.Delta.decode(bytes, true)
  end

  test "decodes data stream of signed integers going in negative direction" do
    integers = [-2, -3, -5, -7, -11, -13, -17, -19, -23, -29]

    bytes = Orc.Chunk.Encoder.Integer.Delta.encode(integers, true)

    assert {integers, <<>>} == Orc.Chunk.Decoder.Integer.Delta.decode(bytes, true)
  end

  test "decodes padded integers" do
    integers = [1, 2, 4]

    bytes = Orc.Chunk.Encoder.Integer.Delta.encode(integers, true)

    {decoded, <<>>} = Orc.Chunk.Decoder.Integer.Delta.decode(bytes, true)

    assert decoded == [1, 2, 4]
  end

  test "returns remaining bytes" do
    integers = Enum.to_list(1..512)

    bytes = Orc.Chunk.Encoder.Integer.Delta.encode(integers, true)
    assert {decoded, <<4, 5, 1>>} = Orc.Chunk.Decoder.Integer.Delta.decode(bytes <> <<4, 5, 1>>, true)

    assert decoded == integers
  end
end
