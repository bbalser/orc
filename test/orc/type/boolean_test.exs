defmodule Orc.Type.BooleanTest do
  use ExUnit.Case
  import TestData

  describe "index/2" do
    test "index create row index for booleans with no nils" do
      booleans = random_booleans(22_000)

      streams =
        Orc.boolean()
        |> Orc.Type.streams(booleans)

      index = Orc.Type.index(Orc.boolean(), streams)

      assert [0, 0, 0, 0] == get_in(index.entry, [Access.at(0), Access.key(:positions)])
      assert [0, 1161, 98, 0] == get_in(index.entry, [Access.at(1), Access.key(:positions)])
      assert [0, 2451, 68, 0] == get_in(index.entry, [Access.at(2), Access.key(:positions)])
    end

    test "index create rows index for booleans with nils" do
      values_per_chunk = 1_024
      chunk_size = 129

      booleans = random_booleans(22_000, nils?: true)
      streams = Orc.Type.streams(Orc.boolean(), booleans)

      index = Orc.Type.index(Orc.boolean(), streams)

      present_num_chunks_1 = div(10_000, values_per_chunk)
      present_index_address_1 = present_num_chunks_1 * chunk_size
      present_leftover_bytes_1 = (10_000 - values_per_chunk * present_num_chunks_1) |> div(8)

      data_num_values_1 = Enum.take(booleans, 10_000) |> Enum.reject(&is_nil/1) |> length()
      data_num_chunks_1 = div(data_num_values_1, values_per_chunk)
      data_index_address_1 = data_num_chunks_1 * chunk_size
      data_leftover_bytes_1 = (data_num_values_1 - values_per_chunk * data_num_chunks_1) |> div(8)

      data_leftover_bits_1 =
        data_num_values_1 - values_per_chunk * data_num_chunks_1 - data_leftover_bytes_1 * 8

      assert [0, 0, 0, 0, 0, 0, 0, 0] ==
               get_in(index.entry, [Access.at(0), Access.key(:positions)])

      assert [
               0,
               present_index_address_1,
               present_leftover_bytes_1,
               0,
               0,
               data_index_address_1,
               data_leftover_bytes_1,
               data_leftover_bits_1
             ] ==
               get_in(index.entry, [Access.at(1), Access.key(:positions)])

      assert [0, 0, 0, 0, 0, 0, 0, 0] ==
               get_in(index.entry, [Access.at(2), Access.key(:positions)])
    end
  end
end
