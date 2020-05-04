defmodule TestData do

  def random_string(length) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end

  def random_numbers(n, opts \\ []) do
    Enum.map(1..n, fn _ ->
      random_number(opts)
    end)
  end

  def random_number(opts \\ []) do
    max = Keyword.get(opts, :max, 1_000_000)
    nils? = Keyword.get(opts, :nils?, false)

    nil? = :rand.uniform(10) == 1

    case nil? && nils? do
      true -> nil
      false -> :rand.uniform(max)
    end
  end

  def random_booleans(n) do
    Enum.map(1..n, fn _ ->
      case :rand.uniform(2) do
        1 -> false
        2 -> true
      end
    end)
  end
end
