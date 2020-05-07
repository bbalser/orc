defmodule Orc.Type.IntegerTest do
  use ExUnit.Case

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

  describe "values/2" do
    test "decodes data_stream into integers" do
      integers = Enum.to_list(1..22)

      streams =
        Orc.Type.streams(Orc.integer(signed?: true), integers)
        |> Enum.map(fn stream -> {stream.kind, Orc.Stream.binary(stream)} end)

      decoded = Orc.Type.values(Orc.integer(signed?: true), streams)

      assert decoded == integers
    end

    test "decodes data_stream and presence stream into integer list" do
      integers = [1, 2, 3, nil, 5, nil]

      streams =
        Orc.Type.streams(Orc.integer(signed?: true), integers)
        |> Enum.map(fn stream -> {stream.kind, Orc.Stream.binary(stream)} end)

      decoded = Orc.Type.values(Orc.integer(signed?: true), streams)

      assert Enum.take(decoded, length(integers)) == integers
    end
  end

end
