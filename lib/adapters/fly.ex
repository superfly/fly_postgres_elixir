defmodule Fly.Postgres.Adapters.Fly do
  @behaviour Fly.Postgres.Adapter

  @impl Fly.Postgres.Adapter
  defdelegate is_primary?(), to: Fly

  @impl Fly.Postgres.Adapter
  @spec exec_on_primary(Atom, [term], [term], Keyword) :: {:ok, term} | {:error, term}
  def exec_on_primary(func, args, opts, adapter_opts) do
    local_repo = Keyword.fetch!(adapter_opts, :local_repo)
    # Default behavior is to wait for replication. If `:await` is set to
    # false/falsey then skip the LSN query and waiting for replication.
    if Keyword.get(opts, :await, true) do
      Fly.Postgres.rpc_and_wait(local_repo, func, args, timeout: timeout(adapter_opts))
    else
      Fly.rpc_primary(local_repo, func, args, timeout: timeout(adapter_opts))
    end
  end

  defp timeout(adapter_opts), do: Keyword.get(adapter_opts, :timeout, 5_000)
end
