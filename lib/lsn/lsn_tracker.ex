defmodule Fly.Postgres.LSN.Tracker do
  @moduledoc """
  Track the current PostgreSQL LSN or Log Sequence Number.

  This is used to determine which portions of the database log have been
  replicated locally. This lets us determine if a specific transaction chunk has
  been replicated to know that some expected data is present.

  The client process doesn't interact directly with the Tracker GenServer. The
  client can `request_notification` or `request_and_await_notification` and the
  Tracker will notify the process when the data replication has been seen.
  """
  use GenServer
  require Logger

  @lsn_table :lsn_tracker_ets_cache
  @request_tab :lsn_tracker_requests

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

    name = Keyword.get(opts, :name, __MODULE__)
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
    table_name = get_table_name(@lsn_table, opts)

    case :ets.lookup(table_name, :repo) do
      [{:repo, repo}] ->
        repo

      [] ->
        nil
    end
  end

  @doc """
  Get the latest cached LSN replay value.

  ## Options

  - `:tracker` - The tracker name to get the latest LSN replay value for. Uses
    the default tracker name. Needs to be provided when multiple trackers are
    used.
  """
  @spec get_last_replay(opts :: keyword()) :: nil | Fly.Postgres.LSN.t()
  def get_last_replay(opts \\ []) do
    # Option for testing: `:override_table_name` - The ETS table name to read the values from.
    table_name = get_table_name(@lsn_table, opts)

    case :ets.lookup(table_name, :last_log_replay) do
      [{:last_log_replay, %Fly.Postgres.LSN{} = stored}] ->
        stored

      [] ->
        nil
    end
  end

  @doc """
  Return if the LSN value was replicated.
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
  process cares about. This allows a process to block and await their data to be
  replicated and be notified as soon as it's detected.
  """
  @spec request_notification(Fly.Postgres.LSN.t(), opts :: keyword()) :: :ok
  def request_notification(%Fly.Postgres.LSN{source: :insert} = lsn, opts \\ []) do
    table_name = get_table_name(@request_tab, opts)

    # This uses the pid of the requesting process
    :ets.insert(table_name, {self(), lsn})
    :ok
  end

  @doc """
  Blocking function that waits for a `request_notification/2` response message
  to be received. The timeout defaults to 5s after which time it stops waiting
  and returns an `{:error, :timeout}` response.

  ## Options

  - `:replication_timeout` - Timeout duration to wait for replication to complete.
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
  - `:replication_timeout` - Timeout duration to wait for replication to complete.
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
          "LSN REQ notification for #{inspect(lsn)}"
        end)

        request_notification(lsn, opts)
        result = await_notification(lsn, opts)

        verbose_log(:info, fn ->
          case result do
            :ready ->
              "LSN RECV tracking notification for #{inspect(lsn)}"

            {:error, :timeout} ->
              "LSN TIMEOUT waiting on #{inspect(lsn)}"
          end
        end)

        result
      end
    end
  end

  defp verbose_log(kind, func) do
    if Application.get_env(:fly_postgres, :verbose_logging) do
      Logger.log(kind, func)
    end
  end

  ###
  ### SERVER CALLBACKS
  ###

  def init(opts) do
    repo = Keyword.fetch!(opts, :repo)
    # name of the tracker process
    tracker_name = Keyword.fetch!(opts, :name)

    # Start with the table names to use for this tracker according to the name of the process.
    default_cache_table_name = tracker_table_name(@lsn_table, tracker_name)
    default_request_table_name = tracker_table_name(@request_tab, tracker_name)

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
    {:ok,
     %{
       tracker_name: tracker_name,
       lsn_table: tab_lsn_cache,
       requests_table: tab_requests,
       frequency: Keyword.get(opts, :frequency, 100),
       repo: repo
     }, {:continue, :initial_query}}
  end

  def handle_continue(:initial_query, state) do
    # perform the initial query to populate the ETS table
    query_last_replay(state)

    # schedule the first check which triggers the ongoing checks
    send(self(), :run_process_notification_requests)

    {:noreply, state}
  end

  def handle_info(:run_process_notification_requests, state) do
    process_request_entries(state)
    # schedule the next check
    Process.send_after(self(), :run_process_notification_requests, state.frequency)
    {:noreply, state}
  end

  # Query for the last replicated log sequence number and write it to the ETS
  # table.
  defp query_last_replay(%{repo: repo} = state) do
    repo
    |> Fly.Postgres.LSN.last_wal_replay()
    |> put_lsn(state)
  end

  @doc false
  # Private function that inserts the most recently seen log replay value into
  # an ETS cache for concurrent read access.
  def put_lsn(%Fly.Postgres.LSN{} = lsn, %{lsn_table: lsn_table} = _state) do
    :ets.insert(lsn_table, {:last_log_replay, lsn})
    lsn
  end

  # Process the list of notification requests in the ETS table. If the tracked
  # insert LSN has been replicated so it is now local, notify the pid and remove
  # the entry.
  @doc false
  # Private function
  def process_request_entries(%{requests_table: requests_table} = state) do
    case fetch_request_entries(requests_table) do
      [] ->
        # Nothing to do. No outstanding requests being tracked
        :ok

      requests ->
        # We have requests to process. Query for the latest replication LSN
        last_replay = query_last_replay(state)
        # Cycle and notify if replicated
        Enum.each(requests, fn {pid, lsn_insert} = entry ->
          # If the tracked LSN was already replicated, notify the pid and remove the
          # entry
          if Fly.Postgres.LSN.replicated?(last_replay, lsn_insert) do
            # notify the requesting pid that the LSN was replicated
            send(pid, {:lsn_replicated, entry})
            # delete the request from the ETS table
            :ets.delete(requests_table, pid)
          end
        end)
    end
  end

  # Return the current list of LSN notification subscriptions.
  # Reads from the ETS table.
  @doc false
  # Private function
  def fetch_request_entries(requests_table \\ @request_tab) do
    :ets.match_object(requests_table, {:"$1", :"$2"})
  end

  # Get the ETS cache tracker table name. Derived from the tracker config
  # becomes easy. Each instance of the tracker has it it's own set of tables.
  @doc false
  # Private function
  def tracker_table_name(base_table_name, tracker_name) when is_atom(tracker_name) do
    # NOTE: This intentionally creates an atom. The input values come from
    # developer code, not user input.
    String.to_atom(Atom.to_string(base_table_name) <> "_" <> Atom.to_string(tracker_name))
  end

  def get_table_name(base_table_name, opts \\ []) do
    tracker_name = Keyword.get(opts, :tracker) || __MODULE__

    Keyword.get_lazy(opts, :override_table_name, fn ->
      tracker_table_name(base_table_name, tracker_name)
    end)
  end
end
