defmodule Orc do
  def boolean(attrs \\ []) do
    Orc.Type.Boolean.new(attrs)
  end

  def byte(attrs \\ []) do
    Orc.Type.Byte.new(attrs)
  end
end
