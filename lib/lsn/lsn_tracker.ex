defmodule Fly.Postgres.LSN.Tracker do
  @moduledoc """
  Tracks the current PostgreSQL LSN or Log Sequence Number. This also tracks
  requests to be notified when replication happens and the requested `:insert`
  LSN was applied locally.

  The GenServer process doesn't have any special behaviors other than creating
  and owning the ETS tables that track the information.

  The module contains functions for writing data to, and reading data from the
  ETS tables.

  Tracking the LSN value is used to determine which portions of the database log
  have been replicated locally. This lets us determine if a specific transaction
  chunk has been replicated to know that some expected data is present.

  The client process doesn't interact directly with the Tracker GenServer. The
  client can `request_notification` or `request_and_await_notification` and the
  requesting processes are notified when the data replication has been seen.
  """
  use GenServer
  require Logger
  import Fly.Postgres, only: [verbose_log: 2]

  alias Fly.Postgres.LSN

  @lsn_table :ets_cache
  @request_table :ets_requests

  ###
  ### CLIENT
  ###

  @doc """
  Start the Tracker that receives work requests.
  """
  def start_link(opts \\ []) do
    if !Keyword.has_key?(opts, :repo) do
      raise ArgumentError, ":repo must be given when starting the LSN Tracker"
    end

    base_name = Keyword.fetch!(opts, :base_name)
    name = get_name(base_name)
    GenServer.start_link(__MODULE__, Keyword.put(opts, :name, name), name: name)
  end

  @doc """
  Get the `Ecto.Repo` used by the tracker.

  ## Options

  - `:tracker` - The tracker name to get the latest LSN replay value for. Uses
    the default tracker name. Needs to be provided when multiple trackers are
    used.
  """
  @spec get_repo(opts :: keyword()) :: nil | module()
  def get_repo(opts \\ []) do
    table_name = get_ets_table_name(@lsn_table, opts)

    case :ets.lookup(table_name, :repo) do
      [{:repo, repo}] ->
        repo

      [] ->
        nil
    end
  end

  @doc """
  Get the latest cached LSN replay value. On a first run, no value is in the
  cache and a `nil` is returned.

  ## Options

  - `:tracker` - The tracker name to get the latest LSN replay value for. Uses
    the default tracker name. Required when using multiple trackers.
  """
  @spec get_last_replay(opts :: keyword()) :: nil | Fly.Postgres.LSN.t()
  def get_last_replay(opts \\ []) do
    # Option for testing: `:override_table_name` - The ETS table name to read the values from.
    table_name = get_ets_table_name(@lsn_table, opts)

    case :ets.lookup(table_name, :last_log_replay) do
      [{:last_log_replay, %Fly.Postgres.LSN{} = stored}] ->
        stored

      [] ->
        nil
    end
  end

  @doc """
  Return if the LSN value was replicated. Compares against the cached value.
  """
  @spec replicated?(Fly.Postgres.LSN.t(), opts :: keyword()) :: boolean()
  def replicated?(%Fly.Postgres.LSN{source: :insert} = lsn, opts \\ []) do
    case get_last_replay(opts) do
      %Fly.Postgres.LSN{} = stored ->
        Fly.Postgres.LSN.replicated?(stored, lsn)

      nil ->
        false
    end
  end

  @doc """
  Request notification for when the database replication includes the LSN the
  process cares about. This enables a process to block and await their data to be
  replicated and be notified as soon as it's detected.

  Adds an entry to ETS table that tracks notification requests.
  """
  @spec request_notification(Fly.Postgres.LSN.t(), opts :: keyword()) :: :ok
  def request_notification(%Fly.Postgres.LSN{source: :insert} = lsn, opts \\ []) do
    verbose_log(:info, fn -> "Requesting replication notification: #{inspect(self())}" end)

    table_name = get_request_tracking_table(opts)

    # This uses the pid of the requesting process
    :ets.insert(table_name, {self(), lsn})
    :ok
  end

  @doc """
  Blocking function that waits for a `request_notification/2` response message
  to be received. The timeout defaults to 5s after which time it stops waiting
  and returns an `{:error, :timeout}` response.

  ## Options

  - `:replication_timeout` - Timeout duration to wait for replication to
    complete. Value is in milliseconds.
  """
  @spec await_notification(Fly.Postgres.LSN.t(), opts :: keyword()) ::
          :ready | {:error, :timeout}
  def await_notification(%Fly.Postgres.LSN{source: :insert} = lsn, opts \\ []) do
    timeout = Keyword.get(opts, :replication_timeout, 5_000)
    pid = self()

    receive do
      {:lsn_replicated, {^pid, ^lsn}} -> :ready
    after
      timeout ->
        {:error, :timeout}
    end
  end

  @doc """
  Request to be notified when the desired level of data replication has
  completed and wait for it to complete. Optionally it may timeout if it takes
  too long.

  ## Options

  - `:tracker` - The name of the tracker to wait on for replication tracking.
  - `:replication_timeout` - Timeout duration to wait for replication to
    complete. Value is in milliseconds.
  """
  @spec request_and_await_notification(Fly.Postgres.LSN.t(), opts :: keyword()) ::
          :ready | {:error, :timeout}

  def request_and_await_notification(%Fly.Postgres.LSN{source: :insert} = lsn, opts \\ []) do
    # Don't register notification request or wait when on the primary
    if Fly.is_primary?() do
      :ready
    else
      # First check if the data is already in the cache. If so, return
      # immediately. Otherwise request to be notified and wait.
      #
      # NOTE: This does add a slight delay to RPC calls using LSN. Waits for the
      # GenServer to run the check for the DB.
      if replicated?(lsn, opts) do
        :ready
      else
        verbose_log(:info, fn ->
          "LSN REQ notification for #{inspect(lsn)} to #{inspect(self())}"
        end)

        request_notification(lsn, opts)
        result = await_notification(lsn, opts)

        verbose_log(:info, fn ->
          case result do
            :ready ->
              "LSN RECV tracking notification for #{inspect(lsn)} to #{inspect(self())}"

            {:error, :timeout} ->
              "LSN TIMEOUT waiting on #{inspect(lsn)} to #{inspect(self())}"
          end
        end)

        result
      end
    end
  end

  ###
  ### SERVER CALLBACKS
  ###

  def init(opts) do
    repo = Keyword.fetch!(opts, :repo)
    # name of the tracker process
    base_name = Keyword.fetch!(opts, :base_name)

    # Start with the table names to use for this tracker according to the name of the process.
    default_cache_table_name = get_ets_table_name(@lsn_table, base_name: base_name)
    default_request_table_name = get_ets_table_name(@request_table, base_name: base_name)

    # TODO: DETERMINE IF THIS OVERRIDE LOGIC IS NEEDED? DELETE?

    # Tests may override the name of the tables. Take override names if given.
    # Otherwise fallback to the default generated names.
    cache_table_name = Keyword.get(opts, :lsn_table_name, default_cache_table_name)
    requests_table_name = Keyword.get(opts, :requests_table_name, default_request_table_name)

    # setup ETS table for caching most recently read DB LSN value
    tab_lsn_cache = :ets.new(cache_table_name, [:named_table, :public, read_concurrency: true])
    # insert special entry for which repo this tracker is using
    :ets.insert(cache_table_name, {:repo, repo})
    # setup ETS table for processes requesting notification when new matching LSN value is seen
    tab_requests = :ets.new(requests_table_name, [:named_table, :public, read_concurrency: true])

    # Initial state. Default to checking every 100msec.
    {
      :ok,
      %{
        base_name: Keyword.get(opts, :base_name),
        name: Keyword.get(opts, :name),
        lsn_table: tab_lsn_cache,
        requests_table: tab_requests,
        repo: repo
      }
    }
  end

  @doc """
  Write the latest LSN value to the cache. Don't record a `nil` LSN value.
  """
  @spec write_lsn_to_cache(nil | LSN.t(), lsn_table :: atom()) :: :ok
  def write_lsn_to_cache(lsn, lsn_table)
  def write_lsn_to_cache(nil, _lsn_table), do: :ok

  def write_lsn_to_cache(%LSN{} = lsn, lsn_table) do
    :ets.insert(lsn_table, {:last_log_replay, lsn})
    :ok
  end

  # Process the list of notification requests in the ETS table. If the tracked
  # insert LSN has been replicated so it is now local, notify the pid and remove
  # the entry.
  @doc false
  # Private function
  def process_request_entries(base_name) do
    req_table = get_request_tracking_table(base_name: base_name)
    lsn_table = get_lsn_cache_table(base_name: base_name)

    case fetch_request_entries(req_table) do
      [] ->
        # Nothing to do. No outstanding requests being tracked
        :ok

      requests ->
        # We have requests to process. Query for the latest replication LSN
        last_replay = get_last_replay(override_table_name: lsn_table)

        # Cycle and notify if replicated
        Enum.each(requests, fn {pid, lsn_insert} = entry ->
          # If the tracked LSN was already replicated, notify the pid and remove the
          # entry
          if Fly.Postgres.LSN.replicated?(last_replay, lsn_insert) do
            # notify the requesting pid that the LSN was replicated
            send(pid, {:lsn_replicated, entry})
            # delete the request from the ETS table
            :ets.delete(req_table, pid)
          end
        end)
    end
  end

  # Return the current list of LSN notification subscriptions.
  # Reads from the ETS table.
  @doc false
  # Private function
  def fetch_request_entries(requests_table) do
    :ets.match_object(requests_table, {:"$1", :"$2"})
  end

  @doc """
  Get the LSN cached ETS table name for the specified tracker.
  """
  @spec get_lsn_cache_table(opts :: keyword()) :: atom()
  def get_lsn_cache_table(opts \\ []) do
    get_ets_table_name(@lsn_table, opts)
  end

  @doc """
  Get the notification request tracking ETS table name for the specified tracker.
  """
  @spec get_request_tracking_table(opts :: keyword()) :: atom()
  def get_request_tracking_table(opts \\ []) do
    get_ets_table_name(@request_table, opts)
  end

  @doc """
  Get the ETS table name. It is derived from the table prefix name and the base
  name of the tracker (as there can be multiple).
  """
  @spec get_ets_table_name(atom(), opts :: keyword()) :: atom()
  def get_ets_table_name(base_table_name, opts \\ []) do
    base_name = Keyword.get(opts, :base_name) || Core.LSN

    Keyword.get_lazy(opts, :override_table_name, fn ->
      # NOTE: This intentionally creates an atom. The input values come from
      # developer code, not user input.
      :"#{base_table_name}_#{get_name(base_name)}"
    end)
  end

  @doc """
  Get the name of the tracker instance that is derived from the base tracking name.
  """
  @spec get_name(atom()) :: atom()
  def get_name(base_name) when is_atom(base_name) do
    :"#{base_name}_tracker"
  end
end
