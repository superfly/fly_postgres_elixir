defmodule Fly.Postgres.FakeRepo do
  # A Fake Ecto.Repo that returns desired fake responses for testing.
  use GenServer
  alias __MODULE__
  alias Fly.Postgres.LSN

  @doc """
  This creates a named GenServer. There can only be 1 running at a time.
  Tests should be set as `async: false`.
  """
  def start_link do
    GenServer.start_link(FakeRepo, nil, name: FakeRepo)
  end

  @impl true
  def init(_) do
    state = %{
      replay_lsn: %LSN{fpart: 0, offset: 1},
      insert_lsn: %LSN{fpart: 0, offset: 1},
      # keep track of how many times the replay and insert numbers were requested
      query_replay_count: 0,
      query_insert_count: 0
    }

    {:ok, state}
  end

  def set_replay_lsn(nil) do
    GenServer.call(FakeRepo, {:set_replay_lsn, nil})
  end

  def set_replay_lsn(%LSN{} = lsn) do
    GenServer.call(FakeRepo, {:set_replay_lsn, LSN.to_text(lsn)})
  end

  def set_insert_lsn(%LSN{} = lsn) do
    GenServer.call(FakeRepo, {:set_insert_lsn, LSN.to_text(lsn)})
  end

  # Query on the replica for the last replayed LSN
  def query!("select CAST(pg_last_wal_replay_lsn() AS TEXT)") do
    lsn_text = GenServer.call(FakeRepo, :get_replay_lsn)
    %Postgrex.Result{rows: [[lsn_text]]}
  end

  # Query on the primary for the newly created LSN for local changes
  def query!("select CAST(pg_current_wal_insert_lsn() AS TEXT)") do
    lsn_text = GenServer.call(FakeRepo, :get_insert_lsn)
    %Postgrex.Result{rows: [[lsn_text]]}
  end

  # Execute the stored procedure that watches for replicate LSN changes
  def query!("SELECT watch_for_lsn_change($1, 2);", [_value]) do
    return_value = GenServer.call(FakeRepo, :get_replay_lsn)
    %Postgrex.Result{rows: [[return_value]]}
  end

  # %Postgrex.Result{
  #   columns: ["watch_for_lsn_change"],
  #   command: :select,
  #   connection_id: 313916,
  #   messages: [],
  #   num_rows: 1,
  #   rows: [[nil]]
  # }

  # %Postgrex.Result{
  #   columns: ["watch_for_lsn_change"],
  #   command: :select,
  #   connection_id: 319654,
  #   messages: [],
  #   num_rows: 1,
  #   rows: [["0/294D9490"]]
  # }

  def query_replay_count do
    GenServer.call(FakeRepo, :query_replay_count)
  end

  def query_insert_count do
    GenServer.call(FakeRepo, :query_insert_count)
  end

  def reset_counts do
    GenServer.call(FakeRepo, :reset_counts)
  end

  ##
  ## SERVER
  ##

  @impl true
  def handle_call(:reset_counts, _from, state) do
    new_state =
      state
      |> Map.put(:query_replay_count, 0)
      |> Map.put(:query_insert_count, 0)

    {:reply, :ok, new_state}
  end

  def handle_call(:query_replay_count, _from, state) do
    {:reply, state.query_replay_count, state}
  end

  def handle_call(:query_insert_count, _from, state) do
    {:reply, state.query_insert_count, state}
  end

  def handle_call(:get_replay_lsn, _from, %{replay_lsn: lsn} = state) do
    {:reply, lsn, Map.put(state, :query_replay_count, state.query_replay_count + 1)}
  end

  def handle_call(:get_insert_lsn, _from, %{insert_lsn: lsn} = state) do
    {:reply, lsn, Map.put(state, :query_insert_count, state.query_replay_count + 1)}
  end

  def handle_call({:set_replay_lsn, lsn}, _from, state) do
    {:reply, lsn, Map.put(state, :replay_lsn, lsn)}
  end

  def handle_call({:set_insert_lsn, lsn}, _from, state) do
    {:reply, lsn, Map.put(state, :insert_lsn, lsn)}
  end

  # Convert an LSN struct back into a text string as it is returned from a query.
  def lsn_to_text(%LSN{fpart: fpart, offset: offset}) do
    Integer.to_string(fpart, 16) <> "/" <> Integer.to_string(offset, 16)
  end
end
