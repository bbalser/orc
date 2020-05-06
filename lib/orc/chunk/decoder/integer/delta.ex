defmodule Orc.Chunk.Decoder.Integer.Delta do

  @spec decode(binary(), boolean()) :: {[integer()], binary}
  def decode(<<3::size(2), width::size(5), length::size(9), data::binary>>, signed?) do
    decoded_width = Orc.Helper.fivebit_decode(width)

    {base_value, rest} = varint(data, signed?)
    {delta_base, rest} = varint(rest, true)

    case width == 0 do
      true ->
        integers =
          Enum.reduce(1..length, [base_value], fn _, [prev | _] = acc ->
            [prev + delta_base | acc]
          end)
          |> Enum.reverse()

        {integers, rest}

      false ->
        sign = if delta_base >= 0, do: 1, else: -1
        bytes_to_read = Orc.Helper.bit_size_to_byte_size(decoded_width * (length - 1))
        <<bytes::binary-size(bytes_to_read), remaining::binary>> = rest

        deltas = [delta_base] ++ get_deltas(bytes, decoded_width, length - 1, sign)

        integers =
          Enum.reduce(deltas, [base_value], fn delta, [prev | _] = acc ->
            [prev + delta | acc]
          end)
          |> Enum.reverse()

        {integers, remaining}
    end
  end

  defp get_deltas(binary, width, count, sign) do
    {values, _} =
      Enum.map_reduce(1..count, binary, fn _, acc ->
        <<delta::size(width), rest::bitstring>> = acc
        {delta * sign, rest}
      end)

    values
  end

  defp varint(binary, signed?) do
    {value, rest} = Varint.LEB128.decode(binary)

    case signed? do
      true -> {Varint.Zigzag.decode(value), rest}
      false -> {value, rest}
    end
  end
end
