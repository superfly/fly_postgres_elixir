defmodule Fly.Postgres do
  @moduledoc """
  Help Elixir applications more easily take advantage of distributed Elixir
  applications using Ecto and PostgreSQL in a primary/replica configuration on
  Fly.io.
  """
  require Logger

  @type env :: :prod | :dev | :test

  @doc """
  Rewrite the database config based on the runtime environment.

  This does not make changes when running a dev or test build.
  """
  @spec config_repo_url(config :: keyword(), env) :: {:ok, keyword()} | no_return()
  def config_repo_url(config, env)

  def config_repo_url(config, :prod) do
    # perform the rewrite when in production environment
    {:ok, rewrite_database_url!(config)}
  end

  def config_repo_url(config, _env) do
    # pass through the config unmodified (dev/test)
    {:ok, config}
  end

  @doc """
  Compute the database url to use for this app given the current configuration
  and runtime environment.
  """
  @spec rewrite_database_url!(config :: keyword()) :: keyword() | no_return()
  def rewrite_database_url!(config) do
    # asserting that the config has a url
    if Keyword.has_key?(config, :url) do
      config
      |> rewrite_host()
      |> rewrite_replica_port()
    else
      raise ArgumentError, "Unable to rewrite database url in fly_postgres. No url was specified."
    end
  end

  @doc """
  Rewrite the `:url` value to include DNS helpers of "top1.nearest.of" to find
  the closes database to target. If the host already contains that, leave it
  unchanged. If it is missing, add it and return the updated the url in the
  config. Raise an exception if the no URL set in the config.
  """
  @spec rewrite_host(config :: keyword()) :: keyword()
  def rewrite_host(config) do
    uri = URI.parse(Keyword.get(config, :url))

    # if detected DNS helpers in the URI, return unchanged
    cond do
      # If using .flycast don't modify
      String.contains?(uri.host, ".flycast") ->
        config

      # If already using `top1.nearest.of.` then don't modify it
      String.contains?(uri.host, "top1.nearest.of.") ->
        config

      # if using `top2.`, rewrite to use `top1.`
      String.contains?(uri.host, "top2.nearest.of.") ->
        new_host = String.replace(uri.host, "top2.nearest.of.", "top1.nearest.of.", global: false)
        updated_uri = %URI{uri | host: new_host} |> URI.to_string()
        Keyword.put(config, :url, updated_uri)

      # No DNS directives detected, add them to the host and return new config
      true ->
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
    start_time = System.os_time(:millisecond)

    {lsn_value, result} =
      Fly.RPC.rpc_region(:primary, __MODULE__, :__rpc_lsn__, [module, func, args, opts],
        timeout: rpc_timeout
      )

    case Fly.Postgres.LSN.Tracker.request_and_await_notification(lsn_value, opts) do
      :ready ->
        verbose_remote_log(:info, fn ->
          "LSN TOTAL rpc_and_wait: #{inspect(System.os_time(:millisecond) - start_time)}msec"
        end)

        result

      {:error, :timeout} ->
        Logger.error(
          "LSN RPC notification timeout calling #{Fly.mfa_string(module, func, args)}}"
        )

        exit(:timeout)
    end
  end

  @doc false
  # Private function executed on the primary
  @spec __rpc_lsn__(module(), func :: atom(), args :: [any()], opts :: Keyword.t()) ::
          {:wal_lookup_failure | Fly.Postgres.LSN.t(), any()}
  def __rpc_lsn__(module, func, args, opts) do
    # Execute the MFA in the primary region
    result = apply(module, func, args)

    # Use `local_repo` here to read most recent WAL value from DB that the
    # caller needs to wait for replication to complete in order to continue and
    # have access to the data.
    # lsn_value = Fly.Postgres.LSN.current_wal_insert(Fly.Postgres.local_repo(opts))
    lsn_value =
      try do
        Fly.Postgres.LSN.current_wal_insert(Fly.Postgres.local_repo(opts))
      rescue
        e in Postgrex.Error ->
          verbose_log(:info, fn ->
            "Current WAL lookup failed: #{inspect(e)}"
          end)

          :wal_lookup_failure
      end

    {lsn_value, result}
  end

  @doc """
  Generate and log "verbose" log messages only if enabled.
  """
  def verbose_log(kind, func) do
    if Application.get_env(:fly_postgres, :verbose_logging) do
      Logger.log(kind, func)
    end
  end

  @doc """
  Generate and log "verbose" log messages only if running on a remote
  (not-primary region) and verbose logging is enabled.
  """
  def verbose_remote_log(kind, func) do
    if Application.get_env(:fly_postgres, :verbose_logging) && !Fly.is_primary?() do
      Logger.log(kind, func)
    end
  end
end
