defmodule Orc do

  @type compressed_binary :: binary

  def boolean(attrs \\ []) do
    Orc.Type.Boolean.new(attrs)
  end
end
