defmodule Orc.Chunk.Encoder.Integer.DeltaTest do
  use ExUnit.Case

  test "encodes some data" do
    integers = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]

    bytes = Orc.Chunk.Encoder.Integer.Delta.encode(integers, false)

    <<encoding::size(2), width::size(5), length::size(9), rest::binary>> = bytes

    assert encoding == 3
    assert Orc.Helper.fivebit_decode(width) == 4
    assert length + 1 == 10

    {base, rest} = Varint.LEB128.decode(rest)
    assert base == 2

    {first_delta, _} = Varint.LEB128.decode(rest)
    assert first_delta == 2

    assert Base.encode16(bytes, case: :lower) == "c609020222424246"
  end

  test "encodes data as signed integers" do
    integers = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]

    bytes = Orc.Chunk.Encoder.Integer.Delta.encode(integers, true)

    <<_::binary-size(2), rest::binary>> = bytes

    {base, rest} = Varint.LEB128.decode(rest)
    assert Varint.Zigzag.decode(base) == 2

    {first_delta, _} = Varint.LEB128.decode(rest)
    assert first_delta == 2
  end

  test "pads out encoded values to binary" do
    integers = [1, 2, 4]

    bytes = Orc.Chunk.Encoder.Integer.Delta.encode(integers, true)

    assert is_binary(bytes)
  end

  test "makes width zero and only uses the delta base when all deltas are the same" do
    integers = 1..100 |> Enum.to_list()

    bytes = Orc.Chunk.Encoder.Integer.Delta.encode(integers, true)

    base_value = Varint.Zigzag.encode(1) |> Varint.LEB128.encode()
    delta_base = Varint.Zigzag.encode(1) |> Varint.LEB128.encode()

    expected = <<3::size(2), 0::size(5), 99::size(9)>> <> base_value <> delta_base

    assert bytes == expected
  end
end
