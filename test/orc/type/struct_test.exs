defmodule Orc.Type.StructTest do
  use ExUnit.Case

  describe "to_list/2" do
    test "should list of types of struct and children" do
      struct =
        Orc.struct(
          children: [
            Orc.integer(name: "one"),
            Orc.boolean(name: "two")
          ]
        )

      [type1, type2, type3] = Orc.Type.to_list(struct)

      assert type1 ==
               Orc.Proto.Type.new(
                 kind: :STRUCT,
                 fieldNames: ["one", "two"],
                 subtypes: [1, 2]
               )

      assert type2 == Orc.Proto.Type.new(kind: :INT)
      assert type3 == Orc.Proto.Type.new(kind: :BOOLEAN)
    end

    test "should list types properly when column is not zero" do
      struct =
        Orc.struct(
          children: [
            Orc.integer(name: "one"),
            Orc.boolean(name: "two")
          ]
        )

      [type1, type2, type3] = Orc.Type.to_list(struct, 7)

      assert type1 ==
               Orc.Proto.Type.new(
                 kind: :STRUCT,
                 fieldNames: ["one", "two"],
                 subtypes: [8, 9]
               )

      assert type2 == Orc.Proto.Type.new(kind: :INT)
      assert type3 == Orc.Proto.Type.new(kind: :BOOLEAN)
    end

    test "should handle nested properties correctly" do
      struct =
        Orc.struct(
          children: [
            Orc.integer(name: "one"),
            Orc.struct(name: "two", children: [Orc.integer(name: "nested")]),
            Orc.boolean(name: "three")
          ]
        )

      [type0, type1, type2, type3, type4] = Orc.Type.to_list(struct)

      assert type0 ==
               Orc.Proto.Type.new(
                 kind: :STRUCT,
                 fieldNames: ["one", "two", "three"],
                 subtypes: [1, 2, 4]
               )

      assert type1 == Orc.Proto.Type.new(kind: :INT)
      assert type2 == Orc.Proto.Type.new(kind: :STRUCT, fieldNames: ["nested"], subtypes: [3])
      assert type3 == Orc.Proto.Type.new(kind: :INT)
      assert type4 == Orc.Proto.Type.new(kind: :BOOLEAN)
    end
  end

  describe "streams/2" do
    test "works for nested structs" do
      struct =
        Orc.struct(
          children: [
            Orc.integer(name: "id", signed?: true),
            Orc.struct(
              name: "spouse",
              children: [
                Orc.integer(name: "id", signed?: true),
                Orc.boolean(name: "alive")
              ]
            ),
            Orc.boolean(name: "alive")
          ]
        )

      data = [
        %{"id" => 1, "spouse" => %{"id" => 10, "alive" => true}, "alive" => false},
        %{"id" => 2, "spouse" => %{"id" => 11, "alive" => false}, "alive" => true},
        %{"id" => 3, "spouse" => %{"id" => 12, "alive" => true}, "alive" => true},
        %{"id" => 4, "spouse" => %{"id" => 13, "alive" => false}, "alive" => false}
      ]

      [int_stream, spouse_int_stream, spouse_bool_stream, bool_stream] =
        Orc.Type.streams(struct, data)

      assert [1, 2, 3, 4] == decode(Orc.integer(signed?: true), int_stream)
      assert [10, 11, 12, 13] == decode(Orc.integer(signed?: true), spouse_int_stream)

      assert [true, false, true, false] =
               decode(Orc.boolean(), spouse_bool_stream) |> Enum.take(4)

      assert [false, true, true, false] = decode(Orc.boolean(), bool_stream) |> Enum.take(4)
    end
  end

  defp decode(type, stream) do
    Orc.Stream.decode(type, Orc.Stream.binary(stream))
  end
end
