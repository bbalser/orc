defmodule Orc.Chunk.Encoder.Integer.Direct do
  @spec encode([integer()], boolean) :: {binary, [integer()]}
  def encode(input, signed?) do
    integers =
      case signed? do
        true -> Enum.map(input, &Varint.Zigzag.encode/1)
        false -> input
      end

    width = Orc.Helper.minimum_bits(integers)
    encoded_width = Orc.Helper.fivebit_encode(width)

    length = length(integers) - 1

    header = <<1::size(2), encoded_width::size(5), length::size(9)>>

    bits =
      Enum.reduce(integers, header, fn int, acc ->
        <<acc::bitstring, int::size(width)>>
      end)

    Orc.Helper.pad_to_binary(bits)
  end
end
