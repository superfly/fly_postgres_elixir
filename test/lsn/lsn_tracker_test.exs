defmodule Fly.Postgres.LSN.TrackerTest do
  # Async false because uses the FakeRepo GenServer to mock out the repo
  use ExUnit.Case, async: false

  doctest Fly.Postgres.LSN.Tracker

  alias Fly.Postgres.LSN
  alias Fly.Postgres.LSN.Tracker
  alias Fly.Postgres.FakeRepo

  @test_lsn_table :test_lsn_cache
  @test_requests :test_lsn_requests

  setup do
    insert_lsn = %Fly.Postgres.LSN{fpart: 0, offset: 2, source: :insert}
    replay_lsn = %Fly.Postgres.LSN{fpart: 0, offset: 1, source: :replay}
    FakeRepo.set_insert_lsn(insert_lsn)
    FakeRepo.set_replay_lsn(replay_lsn)

    server =
      Tracker.start_link(
        tracker_name: :test_tracker,
        lsn_table_name: @test_lsn_table,
        requests_table_name: @test_requests,
        repo: FakeRepo
      )

    state = %{lsn_table: @test_lsn_table, tracker_name: :test_tracker}

    # sleep for a few ms before running tests. Lets the server get started and create
    # the ETS table. Otherwise some race conditions exist where a test may run
    # before the ETS table is created.
    Process.sleep(50)

    %{server: server, insert_lsn: insert_lsn, replay_lsn: replay_lsn, state: state}
  end

  describe "initial query" do
    test "starting new LSN.Tracker queries for initial replay LSN", %{state: state} do
      %LSN{} =
        result =
        Tracker.get_last_replay(tracker: state.tracker_name, override_table_name: state.lsn_table)

      assert result.source == :replay
      assert result.fpart == 0
      assert result.offset == 1
    end
  end

  describe "get_repo/1" do
    test "returns the repo module used by the tracker", %{state: state} do
      assert FakeRepo ==
               Tracker.get_repo(tracker: state.tracker_name, override_table_name: state.lsn_table)
    end
  end

  describe "get_last_replay/1" do
    test "returns initial queries replay value", %{replay_lsn: replay_lsn, state: state} do
      assert replay_lsn ==
               Tracker.get_last_replay(
                 tracker: state.tracker_name,
                 override_table_name: state.lsn_table
               )
    end

    test "returns the stored LSN when found", %{state: state} do
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}
      Tracker.put_lsn(replay, state)

      assert replay ==
               Tracker.get_last_replay(
                 override_table_name: state.lsn_table,
                 tracker: state.tracker_name
               )
    end
  end

  describe "replicated?/2" do
    test "returns true when the replay entry is in the cache", %{state: state} do
      lsn = %LSN{fpart: 0, offset: 100_733_376, source: :insert}
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}
      Tracker.put_lsn(replay, state)

      assert true ==
               Tracker.replicated?(lsn,
                 override_table_name: state.lsn_table,
                 tracker: state.tracker_name
               )
    end

    test "returns false when the replay entry is not present", %{state: state} do
      lsn = %LSN{fpart: 0, offset: 100_733_376, source: :insert}

      assert false ==
               Tracker.replicated?(lsn,
                 override_table_name: state.lsn_table,
                 tracker: state.tracker_name
               )
    end

    test "returns false when a matching entry is not YET present", %{state: state} do
      lsn = %LSN{fpart: 0, offset: 200_000_000, source: :insert}
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}
      Tracker.put_lsn(replay, state)

      assert false ==
               Tracker.replicated?(lsn,
                 override_table_name: state.lsn_table,
                 tracker: state.tracker_name
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

  describe "process_request_entries/1" do
    setup do
      %{
        tracker_name: Tracker,
        lsn_table: @test_lsn_table,
        requests_table: @test_requests,
        repo: FakeRepo
      }
    end

    test "when no requests registered, does nothing", state do
      # first execution, report already replicated
      assert :ok == Tracker.process_request_entries(state)
      refute_received {:lsn_replicated, _any}
    end

    test "when a request is registered but it isn't replicated, nothing sent", state do
      insert = %LSN{fpart: 0, offset: 100_733_376, source: :insert}
      Tracker.request_notification(insert, override_table_name: @test_requests)
      assert :ok == Tracker.process_request_entries(state)
      refute_received {:lsn_replicated, _any}
      # Should still have ETS entry for request
      refute [] == :ets.match_object(@test_requests, {:"$1", :"$2"})
    end

    test "when a request is registered and is replicated, receive notification", state do
      insert = %LSN{fpart: 0, offset: 100_733_376, source: :insert}
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}
      Tracker.request_notification(insert, override_table_name: @test_requests)
      FakeRepo.set_replay_lsn(replay)

      assert :ok == Tracker.process_request_entries(state)
      msg = {:lsn_replicated, {self(), insert}}
      assert_received ^msg
      # the requests table should be empty now
      assert [] == :ets.match_object(@test_requests, {:"$1", :"$2"})
    end
  end

  describe "polling" do
    test "doesn't poll when no subscribers" do
      initial_replay_count = FakeRepo.query_replay_count()
      # sleep to wait and verify no queries are made
      Process.sleep(350)
      assert initial_replay_count == FakeRepo.query_replay_count()
    end

    test "polls when subscribers" do
      initial_replay_count = FakeRepo.query_replay_count()
      insert = %LSN{fpart: 0, offset: 100_733_376, source: :insert}
      # test process subscribes
      Tracker.request_notification(insert, override_table_name: @test_requests)
      # sleep to wait and verify that queries are made
      Process.sleep(350)
      assert FakeRepo.query_replay_count() > initial_replay_count
    end

    test "notifies subscriber once replicated" do
      initial_replay_count = FakeRepo.query_replay_count()
      insert = %LSN{fpart: 0, offset: 100_700_300, source: :insert}
      # test process subscribes
      Tracker.request_notification(insert, override_table_name: @test_requests)

      # Next query returns that it replicated
      FakeRepo.set_replay_lsn(%Fly.Postgres.LSN{fpart: 0, offset: 100_700_300, source: :replay})
      # wait to be notified replication completed. Should return success
      assert :ready ==
               Tracker.await_notification(%Fly.Postgres.LSN{
                 fpart: 0,
                 offset: 100_700_300,
                 source: :insert
               })

      # Verify polling happened
      assert FakeRepo.query_replay_count() > initial_replay_count
    end
  end

  describe "tracker_table_name/2" do
    test "returns combined atom" do
      expected = :"test_lsn_requests_Elixir.Fly.Postgres.LSN.Tracker"
      assert expected == Tracker.tracker_table_name(@test_requests, Tracker)
    end
  end

  describe "perform_lsn_check/3" do
    test "when no change: notifies pid of timeout" do
      FakeRepo.set_replay_lsn(nil)
      Tracker.perform_lsn_check(self(), @test_lsn_table, FakeRepo)
      assert_received :lsn_check_timed_out
    end

    test "when update found: notifies pid of update", %{replay_lsn: replay} do
      lsn = %Fly.Postgres.LSN{fpart: 0, offset: 1_000_000, source: :replay}
      FakeRepo.set_replay_lsn(lsn)
      Tracker.perform_lsn_check(self(), @test_lsn_table, FakeRepo)
      assert_received :lsn_updated
    end

    test "when update found: writes update to LSN ETS table", %{replay_lsn: replay} do
      lsn = %Fly.Postgres.LSN{fpart: 0, offset: 1_000_000, source: :replay}
      FakeRepo.set_replay_lsn(lsn)
      Tracker.perform_lsn_check(self(), @test_lsn_table, FakeRepo)
      # updated in the ETS table
      assert lsn == Tracker.get_last_replay(override_table_name: @test_lsn_table)
    end

    test "when exception: notifies pid of error" do
      FakeRepo.set_replay_lsn(LSN.new("0/9999999", :replay))

      Tracker.perform_lsn_check(self(), @test_lsn_table, FakeRepo)
      assert_received :lsn_check_errored
    end
  end
end
