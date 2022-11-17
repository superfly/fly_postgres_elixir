defmodule Fly.Postgres.LSN.ReaderTest do
  # uses FakeRepo, no async
  use ExUnit.Case, async: false

  doctest Fly.Postgres.LSN.Reader

  alias Fly.Postgres.LSN
  alias Fly.Postgres.FakeRepo
  alias Fly.Postgres.LSN.Reader
  alias Fly.Postgres.LSN.Tracker

  @test_lsn_table :ets_cache_tester_tracker
  @test_requests :ets_requests_tester_tracker
  @base_name :tester

  describe "when running on the primary" do
    test "logs that running on primary and does not start watching" do
      # Default to primary as Los Angeles and local region as same.
      System.put_env("PRIMARY_REGION", "lax")
      System.put_env("MY_REGION", "lax")

      Reader.init(repo: FakeRepo, base_name: @base_name, name: :reader_feeder)
      refute_received :watch_for_lsn_change
    end
  end

  describe "running on replica" do
    setup do
      # running remote in Chicago. Primary is Los Angeles
      System.put_env("PRIMARY_REGION", "lax")
      System.put_env("MY_REGION", "ord")

      insert = %LSN{fpart: 0, offset: 100_733_376, source: :insert}
      replay = %LSN{fpart: 0, offset: 100_733_376, source: :replay}

      state = %{
        name: :tracker,
        base_name: @base_name,
        lsn_table: @test_lsn_table,
        requests_table: @test_requests,
        repo: FakeRepo
      }

      # Init the tracker so the ETS tables get created
      Tracker.init(repo: state.repo, base_name: state.base_name)

      %{state: state, insert_lsn: insert, replay_lsn: replay}
    end

    test "sends self message to watch_for_lsn_change" do
      Reader.init(repo: FakeRepo, base_name: @base_name, name: :reader_feeder)
      assert_received :watch_for_lsn_change
    end

    test "when no update found, sends message to watch again", %{
      replay_lsn: replay,
      state: state
    } do
      # setup repo and cache to match with same LSN value
      FakeRepo.set_replay_lsn(replay)
      Tracker.write_lsn_to_cache(replay, state.lsn_table)
      Reader.handle_info(:watch_for_lsn_change, state)
      assert_received :watch_for_lsn_change
    end

    test "when update found, writes to Tracker's LSN cache", %{
      replay_lsn: replay,
      state: state
    } do
      new_replay = %LSN{replay | offset: replay.offset + 1}
      # setup repo and cache to match with same LSN value
      Tracker.write_lsn_to_cache(replay, state.lsn_table)
      # verify returns pre-updated value
      assert replay == Tracker.get_last_replay(base_name: state.base_name)

      FakeRepo.set_replay_lsn(new_replay)
      Reader.handle_info(:watch_for_lsn_change, state)

      # should write the new DB LSN to the cache and trigger to check again
      assert new_replay == Tracker.get_last_replay(base_name: state.base_name)
      assert_received :watch_for_lsn_change
    end

    test "when update found, notifies pending registered request", %{
      insert_lsn: insert,
      replay_lsn: replay,
      state: state
    } do
      new_replay = %LSN{replay | offset: replay.offset + 1}
      # setup repo and cache to match with same LSN value
      Tracker.write_lsn_to_cache(replay, state.lsn_table)
      # register request to be notified
      Tracker.request_notification(insert, base_name: state.base_name)

      FakeRepo.set_replay_lsn(new_replay)
      Reader.handle_info(:watch_for_lsn_change, state)

      # should write the new DB LSN to the cache and trigger to check again
      assert new_replay == Tracker.get_last_replay(base_name: state.base_name)
      # should be notified that it was received
      pid = self()
      assert_received {:lsn_replicated, {^pid, ^insert}}
    end
  end
end
