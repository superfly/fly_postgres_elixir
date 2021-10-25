defmodule Fly.Postgres.LSN.TrackerTest do
  use ExUnit.Case, async: true

  doctest Fly.Postgres.LSN.Tracker

  alias Fly.Postgres.LSN
  alias Fly.Postgres.LSN.Tracker

  @test_lsn_table :test_lsn_cache
  @test_requests :test_lsn_requests

  setup do
    server =
      Tracker.start_link(
        name: :test_tracker,
        lsn_table_name: @test_lsn_table,
        requests_table_name: @test_requests
      )

    # sleep for 5ms before running tests. Lets the server get started and create
    # the ETS table. Otherwise some race conditions exist where a test may run
    # before the ETS table is created.
    # Process.sleep(5)

    %{server: server}
  end

  describe "get_last_replay/1" do
    test "returns nil when no entry exists" do
      assert nil == Tracker.get_last_replay(@test_lsn_table)
    end

    test "returns the stored LSN when found" do
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}
      Tracker.put_lsn(replay, @test_lsn_table)
      assert replay == Tracker.get_last_replay(@test_lsn_table)
    end
  end

  describe "replicated?/2" do
    test "returns true when the replay entry is in the cache" do
      lsn = %LSN{fpart: 0, offset: 100_733_376, source: :insert}
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}
      Tracker.put_lsn(replay, @test_lsn_table)
      assert true == Tracker.replicated?(@test_lsn_table, lsn)
    end

    test "returns false when the replay entry is not present" do
      lsn = %LSN{fpart: 0, offset: 100_733_376, source: :insert}
      assert false == Tracker.replicated?(@test_lsn_table, lsn)
    end

    test "returns false when a matching entry is not YET present" do
      lsn = %LSN{fpart: 0, offset: 200_000_000, source: :insert}
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}
      Tracker.put_lsn(replay, @test_lsn_table)
      assert false == Tracker.replicated?(@test_lsn_table, lsn)
    end
  end

  describe "request_notification/2" do
    test "writes request to ETS table" do
      lsn = %LSN{source: :insert}
      assert :ok == Tracker.request_notification(@test_requests, lsn)
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
      assert :ready == Tracker.await_notification(lsn, 10)
    end

    test "returns `{:error, :timeout}` when timeout reached" do
      lsn = %LSN{source: :insert}
      assert {:error, :timeout} == Tracker.await_notification(lsn, 1)
    end
  end

  describe "process_request_entries/2" do
    test "when no requests registered, does nothing" do
      # first execution, no entries
      replay = %LSN{fpart: 0, offset: 1, source: :replay}
      assert :ok == Tracker.process_request_entries(replay, @test_requests)
      refute_received {:lsn_replicated, _any}
    end

    test "when a request is registered but it isn't replicated, nothing sent" do
      insert = %LSN{fpart: 0, offset: 100_733_376, source: :insert}
      replay = %LSN{fpart: 0, offset: 1, source: :replay}
      Tracker.request_notification(@test_requests, insert)
      assert :ok == Tracker.process_request_entries(replay, @test_requests)
      refute_received {:lsn_replicated, _any}
      # Should still have ETS entry for request
      refute [] == :ets.match_object(@test_requests, {:"$1", :"$2"})
    end

    test "when a request is registered and is replicated, receive notification" do
      insert = %LSN{fpart: 0, offset: 100_733_376, source: :insert}
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}
      Tracker.request_notification(@test_requests, insert)

      assert :ok == Tracker.process_request_entries(replay, @test_requests)
      msg = {:lsn_replicated, {self(), insert}}
      assert_received ^msg
      # the requests table should be empty now
      assert [] == :ets.match_object(@test_requests, {:"$1", :"$2"})
    end
  end
end
