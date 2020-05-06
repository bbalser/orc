defmodule Orc.Helper do
  @bits [0, 1, 2, 4, 8, 16, 24, 32, 40, 48, 56, 64]

  def minimum_bits(integers) do
    max_bits =
      integers
      |> Enum.map(&Integer.digits(&1, 2))
      |> Enum.map(&length/1)
      |> Enum.max()

    Enum.find(@bits, fn bits -> bits >= max_bits end)
  end

  def pad_to_binary(binary) when is_binary(binary), do: binary

  def pad_to_binary(bitstring) when is_bitstring(bitstring) do
    pad = 8 - rem(bit_size(bitstring), 8)

    <<bitstring::bitstring, 0::size(pad)>>
  end

  def bit_size_to_byte_size(number_of_bits) when is_integer(number_of_bits) do
    case rem(number_of_bits, 8) do
      0 -> div(number_of_bits, 8)
      x -> div(number_of_bits + (8 - x), 8)
    end
  end

  @width_to_encoded %{
    0 => 0,
    1 => 0,
    2 => 1,
    4 => 3,
    8 => 7,
    16 => 15,
    24 => 23,
    32 => 27,
    40 => 28,
    48 => 29,
    56 => 30,
    64 => 31
  }

  @encoded_to_width Enum.map(@width_to_encoded, fn {a, b} -> {b, a} end) |> Map.new()

  def fivebit_encode(width) do
    Map.get(@width_to_encoded, width)
  end

  def fivebit_decode(encoded) do
    Map.get(@encoded_to_width, encoded)
  end
end
