defmodule DemoTest do
  use ExUnit.Case
  use Divo
  import TestData

  @session Prestige.new_session(
             url: "http://localhost:8080",
             user: "testing",
             catalog: "hive",
             schema: "default"
           )

  defp schema() do
    [
      {"alive", "boolean", fn _ -> random_boolean(true) end}
    ]
  end

  @rows 22_000

  setup do
    Process.sleep(5_000)

    :ok
  end

  test "stuffy" do
    create_table()

    data =
      Enum.map(1..@rows, fn i ->
        values =
          Enum.reduce(schema(), [], fn {name, _type, function}, acc ->
            acc ++ ["#{value(function.(i))}"]
          end)
          |> Enum.join(",")

        "(" <> values <> ")"
      end)
      |> Enum.join(",")

    fields =
      Enum.map(schema(), fn {name, _, _} -> name end)
      |> Enum.join(",")

    stmt = "INSERT INTO users(#{fields}) VALUES#{data}"
    IO.inspect(stmt, label: "stmt")
    assert {:ok, _} = Prestige.execute(@session, stmt)

    orc_file =
      ExAws.S3.list_objects("kdp-cloud-storage", prefix: "hive-s3/users")
      |> ExAws.request!()
      |> get_in([:body, :contents])
      |> Enum.map(&Map.get(&1, :key))
      |> Enum.reject(&String.contains?(&1, ".placeholder"))
      |> List.first()

    ExAws.S3.download_file("kdp-cloud-storage", orc_file, "stuff.orc")
    |> ExAws.request!()
  end

  defp create_table() do
    fields =
      Enum.map(schema(), fn {name, type, _} ->
        "#{name} #{type}"
      end)
      |> Enum.join(",")

    assert {:ok, _} =
             Prestige.execute(
               @session,
               "CREATE TABLE users(#{fields})"
             )
  end

  defp value(nil), do: "NULL"
  defp value(integer) when is_integer(integer), do: integer
  defp value(boolean) when is_boolean(boolean), do: boolean
  defp value(value), do: "'#{value}'"
end
