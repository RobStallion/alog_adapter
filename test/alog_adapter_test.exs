defmodule AlogAdapterTest do
  use ExUnit.Case
  doctest AlogAdapter

  test "greets the world" do
    assert AlogAdapter.hello() == :world
  end
end
