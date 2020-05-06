defmodule Orc.Chunk.Encoder.Integer.Delta do

  @spec encode([integer()], boolean) :: {binary, [integer()]}
  def encode(integers, signed?) do
    deltas =
      Enum.chunk_every(integers, 2, 1, :discard)
      |> Enum.map(fn [a, b] -> b - a end)

    width = Orc.Helper.minimum_bits(deltas)
    encoded_width = Orc.Helper.fivebit_encode(width)
    length = length(deltas)

    base_value = List.first(integers) |> encode_varint(signed?)
    delta_base = List.first(deltas) |> encode_varint(true)

    binary =
      case all_same?(deltas) do
        true ->
          build_header(0, length) <> base_value <> delta_base

        false ->
          deltas
          |> Enum.drop(1)
          |> Enum.reduce(
            build_header(encoded_width, length) <> base_value <> delta_base,
          fn delta, acc ->
            delta_abs = abs(delta)
            <<acc::bitstring, delta_abs::size(width)>>
          end
          )
      end

    Orc.Helper.pad_to_binary(binary)
  end

  defp build_header(width, length) do
    <<3::size(2), width::size(5), length::size(9)>>
  end

  defp all_same?(list) do
    first = List.first(list)
    Enum.all?(list, fn element -> element == first end)
  end

  defp encode_varint(integer, signed?) do
    case signed? do
      true -> Varint.Zigzag.encode(integer) |> Varint.LEB128.encode()
      false -> Varint.LEB128.encode(integer)
    end
  end
end
