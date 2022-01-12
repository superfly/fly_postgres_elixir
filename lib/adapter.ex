defmodule Fly.Postgres.Adapter do
  # TODO: depend on ecto only for type informations

  @doc """
  Test if host is the primary database host
  """
  @callback is_primary?() :: boolean

  @doc """
  Execute function on primary database host
  """
  @callback exec_on_primary(Atom, [term], [term], [Keyword]) :: {:ok, term} | {:error, term}
end
