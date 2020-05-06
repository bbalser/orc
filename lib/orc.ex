defmodule Orc do

  @type stream_kind :: :DATA | :LENGTH | :PRESENT
  @type compressed_binary :: binary

  def struct(attrs \\ []) do
    Orc.Type.Struct.new(attrs)
  end

  def integer(attrs \\ []) do
    Orc.Type.Integer.new(attrs)
  end

  def boolean(attrs \\ []) do
    Orc.Type.Boolean.new(attrs)
  end
end
