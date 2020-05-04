defmodule Orc.StreamTest do
  use ExUnit.Case
  import TestData

  setup do
    booleans = random_booleans(3_000_000)

    stream = Orc.Stream.encode(Orc.boolean(), booleans)

    [booleans: booleans, stream: stream]
  end

  test "encodes a list of booleans into a stream", %{stream: stream, booleans: booleans} do
    actuals =
      stream
      |> Orc.Stream.binary()
      |> Orc.Compression.decompress()
      |> Stream.unfold(fn
        <<>> -> nil
        binary -> Orc.Chunk.Decoder.decode(Orc.boolean(), binary)
      end)
      |> Enum.to_list()
      |> List.flatten()

    assert stream.kind == :DATA
    assert booleans == Enum.take(actuals, length(booleans))
  end

  test "decodes stream into values", %{stream: stream, booleans: booleans} do
    binary = Orc.Stream.binary(stream)
    values = Orc.Stream.decode(Orc.boolean(), binary)

    assert booleans = Enum.take(values, length(booleans))
  end
end
