defmodule Orc do
  def byte(attrs \\ []) do
    Orc.Type.Byte.new(attrs)
  end
end
