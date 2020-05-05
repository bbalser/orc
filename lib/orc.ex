defmodule Orc do

  @type stream_kind :: :DATA | :LENGTH | :PRESENT
  @type compressed_binary :: binary

  def boolean(attrs \\ []) do
    Orc.Type.Boolean.new(attrs)
  end
end
