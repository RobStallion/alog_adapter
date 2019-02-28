defmodule AlogAdapter do
  # Inherit all behaviour from Ecto.Adapters.SQL
  use Ecto.Adapters.SQL,
    driver: :postgrex,
    migration_lock: "FOR UPDATE"

  alias Ecto.Adapters.Postgres

  # And provide a custom storage implementation
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  @doc """
  All Ecto extensions for Postgrex.
  """
  def extensions, do: []

  # Support arrays in place of IN
  @impl true
  defdelegate dumpers(arg1, arg2), to: Postgres

  ## Storage API

  @impl true
  defdelegate storage_up(opts), to: Postgres

  @impl true
  defdelegate storage_down(opts), to: Postgres

  @impl true
  defdelegate supports_ddl_transaction?(), to: Postgres

  @impl true
  defdelegate structure_dump(default, config), to: Postgres

  @impl true
  defdelegate structure_load(default, config), to: Postgres

  def execute(adapter_meta, query_meta, query, params, opts) do
    {_cache_atom, _, {_id, query_str}} = query
    
    Ecto.Adapters.SQL.execute(adapter_meta, query_meta, query, params, opts)
  end
end
