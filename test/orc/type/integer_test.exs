defmodule Orc.Type.IntegerTest do
  use ExUnit.Case
  import TestData

  describe "streams/2" do
    test "encodes integers into a data stream" do
      integers = [1,2,3]

      [data_stream] = Orc.Type.streams(Orc.integer(signed?: true), integers)

      assert [1,2,3] == Orc.Stream.decode(Orc.integer(signed?: true), Orc.Stream.binary(data_stream))
    end

    test "encodes integers with nils to a data stream and presence stream" do
      integers = [1, nil, nil, 4, 5]

      streams = Orc.Type.streams(Orc.integer(signed?: true), integers)

      data_stream = Enum.find(streams, fn stream -> stream.kind == :DATA end)
      present_stream = Enum.find(streams, fn stream -> stream.kind == :PRESENT end)

      assert [1, 4, 5] == Orc.Stream.decode(Orc.integer(signed?: true), Orc.Stream.binary(data_stream))

      assert [true, false, false, true, true] ==
        Orc.Stream.decode(Orc.boolean(), Orc.Stream.binary(present_stream))
        |> Enum.take(length(integers))
    end
  end

  describe "index/2" do
    test "create row index for integer stream without nils" do
      integers = random_numbers(22_000)

      streams = Orc.Type.streams(Orc.integer(signed?: true), integers)

      index = Orc.Type.index(Orc.integer(signed?: true), streams)


    end
  end
end
