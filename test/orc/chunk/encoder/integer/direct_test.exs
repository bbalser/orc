defmodule Orc.Chunk.Encoder.Integer.DirectTest do
  use ExUnit.Case

  test "encodes unsigned integers in direct encoding" do
    integers = [23713, 43806, 57005, 48879]
    output = Orc.Chunk.Encoder.Integer.Direct.encode(integers, false)

    <<encoding::size(2), width::size(5), length::size(9), first::size(16), second::size(16),
      third::size(16), fourth::size(16)>> = output

    assert encoding == 1
    assert width == 15
    assert length == 3
    assert first == 23713
    assert second == 43806
    assert third == 57005
    assert fourth == 48879

    assert Base.encode16(output, case: :lower) == "5e035ca1ab1edeadbeef"
  end

  test "encodes signed integers with zigzag algorithm" do
    integers = [-23713, 43806, -57005, 48879]
    output = Orc.Chunk.Encoder.Integer.Direct.encode(integers, true)

    <<encoding::size(2), width::size(5), length::size(9), first::size(24), second::size(24),
      third::size(24), fourth::size(24)>> = output

    assert encoding == 1
    assert width == 23
    assert length == 3
    assert first == Varint.Zigzag.encode(-23713)
    assert second == Varint.Zigzag.encode(43806)
    assert third == Varint.Zigzag.encode(-57005)
    assert fourth == Varint.Zigzag.encode(48879)
  end

  test "encode will pad out to full byte if necessary" do
    integers = [1, 2, 3]
    binary = Orc.Chunk.Encoder.Integer.Direct.encode(integers, true)

    assert is_binary(binary)
  end
end
