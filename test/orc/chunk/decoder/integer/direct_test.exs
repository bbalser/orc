defmodule Orc.Chunk.Decoder.Integer.DirectTest do
  use ExUnit.Case
  import TestData

  test "decodes unsigned integers" do
    integers = [23713, 43806, 57005, 48879]
    encoded = Orc.Chunk.Encoder.Integer.Direct.encode(integers, false)

    {decoded, <<>>} = Orc.Chunk.Decoder.Integer.Direct.decode(encoded, false)

    assert integers == decoded
  end

  test "decodes signed integers" do
    integers = [23713, -43806, 57005, 48879]
    encoded = Orc.Chunk.Encoder.Integer.Direct.encode(integers, true)

    {decoded, <<>>} = Orc.Chunk.Decoder.Integer.Direct.decode(encoded, true)

    assert integers == decoded
  end

  test "decodes only up to length integers and returns remaining binary" do
    integers = random_numbers(9)
    encoded = Orc.Chunk.Encoder.Integer.Direct.encode(integers, true)

    {result, <<1, 2, 3>>} = Orc.Chunk.Decoder.Integer.Direct.decode(encoded <> <<1, 2, 3>>, true)

    assert result == integers
  end

  test "decodes all the bytes including padding" do
    integers = [1, 2, 3]
    binary = Orc.Chunk.Encoder.Integer.Direct.encode(integers, true)

    assert {decoded, <<8, 5, 4>>} =
             Orc.Chunk.Decoder.Integer.Direct.decode(binary <> <<8, 5, 4>>, true)

    assert decoded == [1, 2, 3]
  end
end
