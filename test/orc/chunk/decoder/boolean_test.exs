defmodule Orc.Chunk.Decoder.BooleanTest do
  use ExUnit.Case

  test "decodeing ff80 into 1 true followed by 7 false" do
    bytes = Base.decode16!("ff80", case: :lower)

    {decoded, <<67, 89>>} = Orc.Chunk.Decoder.decode(Orc.boolean(), bytes <> <<67, 89>>)

    assert decoded == [true] ++ Enum.map(1..7, fn _ -> false end)
  end

end
