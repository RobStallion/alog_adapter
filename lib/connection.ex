defmodule AlogAdapter.Connection do
  @moduledoc false
  alias Ecto.Adapters.Postgres.Connection, as: PC

  @default_port 5432
  @behaviour Ecto.Adapters.SQL.Connection

  ## Module and Options

  @impl true
  def child_spec(opts) do
    opts
    |> Keyword.put_new(:port, @default_port)
    |> Postgrex.child_spec()
  end

  @impl true
  defdelegate to_constraints(error_struct), to: PC

  @impl true
  defdelegate prepare_execute(conn, name, sql, params, opts), to: PC

  @impl true
  defdelegate query(conn, sql, params, opts), to: PC

  @impl true
  defdelegate execute(conn, query, params, opts), to: PC

  @impl true
  defdelegate stream(conn, sql, params, opts), to: PC

  import Ecto.Query

  @impl true
  def all(query) do
    query = from m in query, distinct: m.comment_id_no
    Ecto.Adapters.Postgres.Connection.all(query)
  end

  @impl true
  defdelegate update_all(query, prefix \\ nil), to: PC

  @impl true
  defdelegate delete_all(query), to: PC

  @impl true
  defdelegate insert(prefix, table, header, rows, on_conflict, returning), to: PC

  @impl true
  defdelegate update(prefix, table, fields, filters, returning), to: PC

  @impl true
  defdelegate delete(prefix, table, filters, returning), to: PC

  @impl true
  defdelegate execute_ddl(arg), to: PC

  @impl true
  defdelegate ddl_logs(result), to: PC
end
