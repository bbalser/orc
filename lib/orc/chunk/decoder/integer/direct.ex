defmodule Orc.Chunk.Decoder.Integer.Direct do
  @spec decode(binary(), boolean()) :: {[integer()], binary}
  def decode(<<1::size(2), width::size(5), length::size(9), data::bitstring>>, signed?) do
    decoded_width = Orc.Helper.fivebit_decode(width)
    length = length + 1

    bytes_to_read = Orc.Helper.bit_size_to_byte_size(decoded_width * length)
    <<bytes::binary-size(bytes_to_read), remaining::binary>> = data

    {values, _} =
      Enum.map_reduce(1..length, bytes, fn _, bytes ->
        <<value::size(decoded_width), rest::bitstring>> = bytes
        {value, rest}
      end)

    case signed? do
      true -> {Enum.map(values, &Varint.Zigzag.decode/1), remaining}
      false -> {values, remaining}
    end
  end
end
