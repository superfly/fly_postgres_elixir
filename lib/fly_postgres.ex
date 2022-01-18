defmodule Fly.Postgres do
  @moduledoc """
  Help Elixir applications more easily take advantage of distributed Elixir
  applications using Ecto and PostgreSQL in a primary/replica configuration on
  Fly.io.
  """
  require Logger

  @spec config_repo_url(config :: keyword()) :: {:ok, keyword()}
  def config_repo_url(config) do
    # If the config contains a database URL, we'll use that to potentially
    # re-write to hit the replica if in a replica region.
    if Keyword.has_key?(config, :url) do
      {:ok, Fly.Postgres.rewrite_database_url(config)}
    else
      {:ok, config}
    end
  end

  @doc """
  Compute the database url to use for this app given the current configuration
  and runtime environment.
  """
  @spec rewrite_database_url(config :: keyword()) :: keyword()
  def rewrite_database_url(config) do
    # if running on the primary, return config unchanged
    if Fly.is_primary?() do
      config
    else
      # change the config to connect to replica
      Keyword.put(config, :url, replica_db_url(Keyword.get(config, :url)))
    end
  end

  @doc """
  Return a database url used for connecting to a replica database. Changes the
  port to target a replica instance that is assumed to be available.
  """
  @spec replica_db_url(url :: String.t()) :: String.t()
  def replica_db_url(url) do
    # Infer the replica URL. Change the port to target a replica instance.
    uri = URI.parse(url)
    replica_uri = %URI{uri | port: 5433}
    URI.to_string(replica_uri)
  end

  @doc """
  Returns the Repo module used by the application that is not the wrapped
  version. Used for making direct writable calls.

  ## Example

  Requires using application to configure.

      # Configure database repository
      config :fly_postgres, :local_repo, MyApp.Repo.Local

  """
  def local_repo do
    Application.fetch_env!(:fly_postgres, :local_repo)
  end

  @doc """
  Function used to make the repository be read-only and error when creates,
  updates, or deletes are attempted. This behaves like a read-only replica
  which is helpful when modelling that setup locally in a dev environment.

  ## Example

  In your `config/dev.exs`,

      # Configure your database
      config :my_app, MyApp.Repo.Local,
        username: "postgres",
        password: "postgres",
        database: "my_db_dev",
        hostname: "localhost",
        show_sensitive_data_on_connection_error: true,
        # Forcing the repo to be R/O locally for dev testing
        after_connect: {Fly, :make_connection_read_only!, []},
        pool_size: 10

  """
  @spec make_connection_read_only!(DBConnection.t()) :: :ok | no_return()
  def make_connection_read_only!(conn) do
    # This can be done directly in the Repo config as this.
    # after_connect: {Postgrex, :query!, ["SET default_transaction_read_only = on", []]},
    Postgrex.query!(conn, "SET default_transaction_read_only = on", [])
    :ok
  end

  @doc """
  Execute the MFA (Module, Function, Arguments) on a node in the primary region.
  This waits for the data to be replicated to the current node before continuing
  on.

  This presumes the primary region has direct access to a writable primary
  Postgres database.
  """
  def rpc_and_wait(module, func, args, opts \\ []) do
    {lsn_value, result} =
      Fly.RPC.rpc_region(:primary, __MODULE__, :__rpc_lsn__, [module, func, args], opts)

    case Fly.Postgres.LSN.Tracker.request_and_await_notification(lsn_value) do
      :ready ->
        result

      {:error, :timeout} ->
        Logger.error("RPC notification timeout calling #{Fly.mfa_string(module, func, args)}}")
        exit(:timeout)
    end
  end

  @doc false
  # Private function executed on the primary
  def __rpc_lsn__(module, func, args) do
    # Execute the MFA in the primary region
    result = apply(module, func, args)

    # Use `local_repo` here to read most recent WAL value from DB that the
    # caller needs to wait for replication to complete in order to continue and
    # have access to the data.
    lsn_value = Fly.Postgres.LSN.current_wal_insert(Fly.Postgres.local_repo())

    {lsn_value, result}
  end
end
