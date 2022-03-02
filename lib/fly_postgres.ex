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
    case Keyword.fetch(config, :url) do
      {:ok, _url} ->
        {:ok, rewrite_database_url(config)}

      :error ->
        # :url key not found. Likely local dev/testing. Return unchanged.
        {:ok, config}
    end
  end

  @doc """
  Compute the database url to use for this app given the current configuration
  and runtime environment.
  """
  @spec rewrite_database_url(config :: keyword()) :: keyword()
  def rewrite_database_url(config) do
    config
    |> rewrite_host()
    |> rewrite_replica_port()
  end

  @doc """
  Rewrite the `:url` value to include DNS helpers of "top1.nearest.of" to find
  the closes database to target. If the host already contains that, leave it
  unchanged. If it is missing, add it and return the updated the url in the
  config.
  """
  @spec rewrite_host(config :: keyword()) :: keyword()
  def rewrite_host(config) do
    uri = URI.parse(Keyword.get(config, :url))

    # if detected DNS helpers in the URI, return unchanged
    cond do
      String.contains?(uri.host, "top1.nearest.of.") ->
        config

      String.contains?(uri.host, "top2.nearest.of.") ->
        new_host = String.replace(uri.host, "top2.nearest.of.", "top1.nearest.of.", global: false)
        updated_uri = %URI{uri | host: new_host} |> URI.to_string()
        Keyword.put(config, :url, updated_uri)

      true ->
        # Not detected. Add them to the Host and return new config with replaced
        # host that includes DNS helpers
        updated_uri = %URI{uri | host: "top1.nearest.of.#{uri.host}"} |> URI.to_string()
        Keyword.put(config, :url, updated_uri)
    end
  end

  @doc """
  Rewrite the `:url` value to target the Postgres replica port of 5433.
  """
  @spec rewrite_replica_port(config :: keyword()) :: keyword()
  def rewrite_replica_port(config) do
    # if running on the primary, return config unchanged
    if Fly.is_primary?() do
      config
    else
      # Infer the replica URL. Change the port to target a replica instance.
      uri = URI.parse(Keyword.get(config, :url))
      replica_uri = %URI{uri | port: 5433}
      updated_uri = URI.to_string(replica_uri)
      Keyword.put(config, :url, updated_uri)
    end
  end

  @doc """
  Returns the Repo module used by the Tracker that is not the wrapped
  version. Used for making direct writable calls.

  ## Example

  Application is used to configure the tracker.

      # Given Application config like this:
      {Fly.Postgres.LSN.Tracker, repo: MyApp.Repo.Local}

      Fly.Postgres()
      #=> MyApp.Repo.Local

      Fly.Postgres(tracker: Fly.Postgres.LSN.Tracker)
      #=> MyApp.Repo.Local

      # Given Application config like this:
      {Fly.Postgres.LSN.Tracker, repo: MyApp.Repo.Local_1, name: :repo_tracker_1},
      {Fly.Postgres.LSN.Tracker, repo: MyApp.Repo.Local_2, name: :repo_tracker_2},

      Fly.Postgres(tracker: :repo_tracker_1)
      #=> MyApp.Repo.Local_1

      Fly.Postgres(tracker: :repo_tracker_2)
      #=> MyApp.Repo.Local_2
  """
  def local_repo(opts \\ []) do
    Fly.Postgres.LSN.Tracker.get_repo(opts)
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

  ## Options

  - `:tracker` - The name of the tracker to wait on for replication tracking.
  - `:rpc_timeout` - Timeout duration to wait for RPC call to complete
  - `:replication_timeout` - Timeout duration to wait for replication to complete.
  """
  def rpc_and_wait(module, func, args, opts \\ []) do
    rpc_timeout = Keyword.get(opts, :rpc_timeout, 5_000)

    {lsn_value, result} =
      Fly.RPC.rpc_region(:primary, __MODULE__, :__rpc_lsn__, [module, func, args, opts],
        timeout: rpc_timeout
      )

    case Fly.Postgres.LSN.Tracker.request_and_await_notification(lsn_value, opts) do
      :ready ->
        result

      {:error, :timeout} ->
        Logger.error("RPC notification timeout calling #{Fly.mfa_string(module, func, args)}}")
        exit(:timeout)
    end
  end

  @doc false
  # Private function executed on the primary
  def __rpc_lsn__(module, func, args, opts) do
    # Execute the MFA in the primary region
    result = apply(module, func, args)

    # Use `local_repo` here to read most recent WAL value from DB that the
    # caller needs to wait for replication to complete in order to continue and
    # have access to the data.
    lsn_value = Fly.Postgres.LSN.current_wal_insert(Fly.Postgres.local_repo(opts))

    {lsn_value, result}
  end
end
