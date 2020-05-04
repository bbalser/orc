defmodule Orc.Chunk.Encoder.ByteTest do
  use ExUnit.Case

  test "literal encoding" do
    bytes = Base.decode16!("4445", case: :lower)

    encoded = Orc.Chunk.Encoder.encode(Orc.byte(), bytes)

    <<header::integer-signed-size(8), rest::binary>> = encoded

    assert header ==  - byte_size(bytes)
    assert rest == bytes
    assert Base.encode16(encoded, case: :lower) == "fe4445"
  end
end
