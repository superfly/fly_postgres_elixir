defmodule Fly.Postgres.LSN.Reader do
  @moduledoc """
  Watches the configured database for replication changes.

  When a change is found, it writes it to an ETS cache and notifies
  `Fly.Postgres.LSN.Tracker` that an update was received.
  """
  use GenServer
  require Logger

  alias Fly.Postgres.LSN
  alias Fly.Postgres.LSN.Tracker

  ###
  ### CLIENT
  ###

  @doc """
  Start the Reader that performs DB replication requests.
  """
  def start_link(opts \\ []) do
    if !Keyword.has_key?(opts, :repo) do
      raise ArgumentError, ":repo must be given when starting the LSN Reader"
    end

    name = get_name(Keyword.fetch!(opts, :base_name))
    GenServer.start_link(__MODULE__, Keyword.put(opts, :name, name), name: name)
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
    base_name = Keyword.fetch!(opts, :base_name)
    reader_name = Keyword.fetch!(opts, :name)

    verbose_log(:info, fn -> "LSN Reader #{reader_name} starting" end)

    # name of the tracker process
    # tracker_name = Tracker.get_name(base_name)

    IO.inspect(opts, label: "READER OPTS")
    lsn_cache_table = Tracker.get_lsn_cache_table(opts)
    requests_table = Tracker.get_request_tracking_table(opts)

    # if conditions are right, request to start watching for LSN changes
    conditionally_start_watching()

    # Initial state. Default to checking every 100msec.
    {:ok,
     %{
       name: reader_name,
       base_name: base_name,
       #  tracker_name: tracker_name,
       lsn_table: lsn_cache_table,
       requests_table: requests_table,
       repo: repo
     }}
  end

  def handle_info(:watch_for_lsn_change, state) do
    # TODO: Read the current LSN from the cache
    last_lsn = Tracker.get_last_replay(base_name: state.base_name)

    # execute stored procedure
    case LSN.last_wal_replay_watch(state.repo, last_lsn) do
      nil ->
        # nothing to do
        :ok

      %LSN{} = new_lsn ->
        # TODO: write the update LSN to the cache and process any pending requests
        Tracker.write_lsn_to_cache(new_lsn, state.lsn_table)
        Tracker.process_request_entries(state.base_name)
        :ok
    end

    # trigger self to check again
    send(self(), :watch_for_lsn_change)

    {:noreply, state}
  end

  #
  def conditionally_start_watching() do
    if Fly.is_primary?() do
      Logger.info("Detected running on primary. No local replication to track.")
    else
      # request the watching procedure to start
      send(self(), :watch_for_lsn_change)
    end
  end

  # ###
  # ### ASYNC PROCESS FUNCTIONS
  # ###

  # @doc """
  # Executes long running (looping) stored procedure that watches for LSN
  # replication updates.
  # """
  # def perform_lsn_check(server_pid, lsn_table, repo) do
  #   # read the last known LSN value from the lsn_table
  #   last_replay = get_last_replay(override_table_name: lsn_table)

  #   try do
  #     # Execute stored procedure using "from" lsn value. Query can fail if DB
  #     # connection is severed while the long-running (looping) stored procedure
  #     # is working. Detect the DB connection crash and handle it. Notify server
  #     # pid and let the process die.
  #     case Fly.Postgres.LSN.last_wal_replay_watch(repo, last_replay) do
  #       nil ->
  #         # No change by the end of the timeout
  #         send(server_pid, :lsn_check_timed_out)

  #       %LSN{} = lsn ->
  #         # store LSN result
  #         write_lsn_to_cache(lsn, lsn_table)
  #         # notify server_pid of result
  #         send(server_pid, :lsn_updated)
  #     end
  #   rescue
  #     error ->
  #       Logger.error("Failure checking for LSN update in DB. Error: #{inspect(error)}")
  #       send(server_pid, :lsn_check_errored)
  #   end
  # end

  @doc """
  Get the name of the reader instance that is derived from the base tracking name.
  """
  @spec get_name(atom()) :: atom()
  def get_name(base_name) when is_atom(base_name) do
    :"#{base_name}_reader"
  end
end
