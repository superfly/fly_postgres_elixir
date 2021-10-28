defmodule Fly.Postgres do
  @moduledoc """
  Help Elixir applications more easily take advantage of distributed Elixir
  applications using Ecto and PostgreSQL in a primary/replica configuration on
  Fly.io.
  """
  require Logger

  @doc """
  Return the database url used for connecting to the primary database. This is
  provided by the Fly.io platform when you have attached to a PostgreSQL
  database. Stored as an ENV called `DATABASE_URL`.
  """
  def primary_db_url do
    raw_url = System.fetch_env!("DATABASE_URL")
    primary = Fly.primary_region()

    # Be more explicit with the primary DB host name to specify the region.
    # Otherwise DNS might direct it somewhere else.
    uri = URI.parse(raw_url)
    primary_uri = %URI{uri | host: "#{primary}.#{uri.host}"}
    URI.to_string(primary_uri)
  end

  @doc """
  Return a database url used for connecting to a replica database. This makes
  the assumption that there is a replica running in the region where the app
  instance is running.
  """
  def replica_db_url() do
    raw_url = System.fetch_env!("DATABASE_URL")
    current = Fly.my_region()

    # Infer the replica URL. Assumed to be running in the region the app is
    # deployed to.
    uri = URI.parse(raw_url)
    replica_uri = %URI{uri | host: "#{current}.#{uri.host}", port: 5433}
    URI.to_string(replica_uri)
  end

  @doc """
  Compute the database url to use for this app given the current configuration
  and runtime environment.
  """
  def database_url do
    data = %{
      primary: Fly.primary_region(),
      current: Fly.my_region(),
      primary_url: primary_db_url(),
      replica_url: replica_db_url()
    }

    # Only check for non-primary DB URL in prod build
    if System.get_env("MIX_ENV") == "prod" do
      do_database_url(data)
    else
      Logger.info("Using raw DATABASE_URL. Assumed DEV or TEST environment")
      System.fetch_env!("DATABASE_URL")
    end
  end

  defp do_database_url(%{primary: pri, current: curr} = data) when pri == curr do
    Logger.info("Primary DB connection - Running in primary region")
    data.primary_url
  end

  defp do_database_url(%{} = data) do
    Logger.info("Replica DB connection - Using replica")
    data.replica_url
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
