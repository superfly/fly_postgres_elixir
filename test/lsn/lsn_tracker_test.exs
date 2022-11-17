defmodule Fly.Postgres.LSN.TrackerTest do
  # Async false because uses the FakeRepo GenServer to mock out the repo
  use ExUnit.Case, async: false

  doctest Fly.Postgres.LSN.Tracker

  alias Fly.Postgres.LSN
  alias Fly.Postgres.LSN.Tracker
  alias Fly.Postgres.FakeRepo

  # ETS table names are derived to these value from the "base_name" and internal
  # code.
  @test_lsn_table :ets_cache_tester_tracker
  @test_requests :ets_requests_tester_tracker
  @base_name :tester

  setup do
    insert_lsn = %Fly.Postgres.LSN{fpart: 0, offset: 2, source: :insert}
    replay_lsn = %Fly.Postgres.LSN{fpart: 0, offset: 1, source: :replay}
    FakeRepo.set_insert_lsn(insert_lsn)
    FakeRepo.set_replay_lsn(replay_lsn)

    # Starting the tracker creates the ETS tables
    {:ok, server} =
      Tracker.start_link(
        name: :test_tracker,
        base_name: @base_name,
        repo: FakeRepo
      )

    # sleep for a few ms before running tests. Lets the server get started and create
    # the ETS table. Otherwise some race conditions exist where a test may run
    # before the ETS table is created.
    Process.sleep(50)
    # Get the GenServer's internal state to use in function calls
    state = :sys.get_state(server)

    %{server: server, insert_lsn: insert_lsn, replay_lsn: replay_lsn, state: state}
  end

  describe "get_repo/1" do
    test "returns the repo module used by the tracker", %{state: state} do
      assert FakeRepo ==
               Tracker.get_repo(tracker: state.name, override_table_name: state.lsn_table)
    end
  end

  describe "get_last_replay/1" do
    test "returns the stored LSN when found", %{state: state} do
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}
      Tracker.write_lsn_to_cache(replay, state.lsn_table)

      assert replay ==
               Tracker.get_last_replay(
                 override_table_name: state.lsn_table,
                 tracker: state.name
               )
    end
  end

  describe "replicated?/2" do
    test "returns false when no last replay found in the cache", %{state: state} do
      lsn = %LSN{fpart: 0, offset: 100_733_376, source: :insert}
      Tracker.write_lsn_to_cache(nil, state.lsn_table)

      assert false ==
               Tracker.replicated?(lsn,
                 override_table_name: state.lsn_table,
                 tracker: state.name
               )
    end

    test "returns true when the replay entry is in the cache", %{state: state} do
      lsn = %LSN{fpart: 0, offset: 100_733_376, source: :insert}
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}
      Tracker.write_lsn_to_cache(replay, state.lsn_table)

      assert true ==
               Tracker.replicated?(lsn,
                 override_table_name: state.lsn_table,
                 tracker: state.name
               )
    end

    test "returns false when the replay entry is not present", %{state: state} do
      lsn = %LSN{fpart: 0, offset: 100_733_376, source: :insert}

      assert false ==
               Tracker.replicated?(lsn,
                 override_table_name: state.lsn_table,
                 tracker: state.name
               )
    end

    test "returns false when a matching entry is not YET present", %{state: state} do
      lsn = %LSN{fpart: 0, offset: 200_000_000, source: :insert}
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}
      Tracker.write_lsn_to_cache(replay, state.lsn_table)

      assert false ==
               Tracker.replicated?(lsn,
                 override_table_name: state.lsn_table,
                 tracker: state.name
               )
    end
  end

  describe "request_notification/2" do
    test "writes request to ETS table" do
      lsn = %LSN{source: :insert}
      assert :ok == Tracker.request_notification(lsn, override_table_name: @test_requests)
      entries = :ets.match_object(@test_requests, {:"$1", :"$2"})
      [{pid, tracked_lsn}] = entries
      assert pid == self()
      assert tracked_lsn == lsn
    end
  end

  describe "await_notification/2" do
    test "returns `:ready` when notification received" do
      lsn = %LSN{source: :insert}
      # send the message before starting to ensure it's there since the await
      # call will block
      send(self(), {:lsn_replicated, {self(), lsn}})
      assert :ready == Tracker.await_notification(lsn, replication_timeout: 10)
    end

    test "returns `{:error, :timeout}` when timeout reached" do
      lsn = %LSN{source: :insert}
      assert {:error, :timeout} == Tracker.await_notification(lsn, replication_timeout: 1)
    end
  end

  describe "request_and_await_notification/2" do
    setup do
      # Default to primary as Los Angeles and local region as Chicago.
      System.put_env("PRIMARY_REGION", "lax")
      System.put_env("MY_REGION", "ord")
      %{lsn: %LSN{fpart: 0, offset: 200_000_000, source: :insert}}
    end

    test "returns :ready when run on the primary", %{lsn: lsn} do
      # set the current region to be the primary region
      System.put_env("MY_REGION", "lax")

      assert :ready ==
               Tracker.request_and_await_notification(lsn, base_name: @base_name)
    end

    test "returns :ready when the LSN is already in replication cache", %{lsn: lsn} do
      # record the insert LSN as having been replicated and cached
      replay = %LSN{lsn | source: :replay}
      Tracker.write_lsn_to_cache(replay, @test_lsn_table)

      assert :ready ==
               Tracker.request_and_await_notification(lsn, base_name: @base_name)
    end

    test "when not cached, registers notification request", %{lsn: lsn} do
      result =
        Tracker.request_and_await_notification(lsn,
          base_name: @base_name,
          replication_timeout: 1
        )

      # it times out
      assert result == {:error, :timeout}
      # request is tracked for the test process and the LSN
      [request] = Tracker.fetch_request_entries(@test_requests)
      assert {self(), lsn} == request
    end

    test "when receives replication notification, returns :ready", %{lsn: lsn} do
      # send the test process the expected message so the "await"
      # receives it and believes the LSN was replicated
      send(self(), {:lsn_replicated, {self(), lsn}})

      # check and wait for 1 second (re-checks)
      result =
        Tracker.request_and_await_notification(lsn,
          base_name: @base_name,
          replication_timeout: 1_000
        )

      assert result == :ready
    end

    test "when no notification received, returns timeout error", %{lsn: lsn} do
      assert {:error, :timeout} ==
               Tracker.request_and_await_notification(lsn,
                 base_name: @base_name,
                 replication_timeout: 1
               )
    end
  end

  describe "process_request_entries/1" do
    test "when no requests registered, does nothing" do
      # ensure there are no pending notification requests
      assert [] == Tracker.fetch_request_entries(@test_requests)

      # executing return :ok with nothing to do
      assert :ok == Tracker.process_request_entries(@base_name)
      refute_received {:lsn_replicated, _any}
    end

    test "when a request is registered but it isn't replicated, nothing sent", %{
      state: state,
      insert_lsn: insert,
      replay_lsn: replay
    } do
      # write the older replay to the cache. Insert is newer, so it hasn't been
      # replicated yet.
      Tracker.write_lsn_to_cache(replay, state.lsn_table)

      :ok = Tracker.request_notification(insert, override_table_name: @test_requests)
      assert :ok == Tracker.process_request_entries(@base_name)
      refute_received {:lsn_replicated, _any}
      # Should still have ETS entry for request
      assert [] != Tracker.fetch_request_entries(@test_requests)
    end

    test "when a request is registered and is replicated, receive notification", %{
      state: state,
      insert_lsn: insert
    } do
      insert_replicated = %LSN{insert | source: :replay}
      # write into the cache the insert LSN as having been replicated.
      Tracker.write_lsn_to_cache(insert_replicated, state.lsn_table)

      :ok = Tracker.request_notification(insert, override_table_name: @test_requests)

      assert :ok == Tracker.process_request_entries(@base_name)
      msg = {:lsn_replicated, {self(), insert}}
      assert_received ^msg
      # the requests table should be empty now
      assert [] == Tracker.fetch_request_entries(@test_requests)
    end
  end
end
