defmodule Orc.Type.Byte do
  defstruct [:name]

  def new(attrs) do
    struct!(__MODULE__, attrs)
  end
end
