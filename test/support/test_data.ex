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

  def random_booleans(n, opts \\ []) do
    rand_limit = if Keyword.get(opts, :nils?, false), do: 3, else: 2
    Enum.map(1..n, fn _ ->
      case :rand.uniform(rand_limit) do
        1 -> false
        2 -> true
        3 -> nil
      end
    end)
  end

  def random_boolean(nil? \\ false)

  def random_boolean(true) do
    case :rand.uniform(3) do
      1 -> false
      2 -> true
      3 -> nil
    end
  end

  def random_boolean(false) do
    :rand.uniform(2) == 1
  end
end
