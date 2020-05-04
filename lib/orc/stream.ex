defmodule Orc.Stream do

  @type t :: %__MODULE__{
    type: Orc.Type.t(),
    kind: :DATA | :LENGTH | :PRESENT,
    blocks: [Orc.CompressedBlock.t()]
  }

  defstruct type: nil, kind: nil, blocks: []

  def new(attrs) do
    struct!(__MODULE__, attrs)
  end

end
