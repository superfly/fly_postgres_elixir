defmodule Fly.Postgres.LSNTest do
  # uses FakeRepo
  use ExUnit.Case, async: false

  doctest Fly.Postgres.LSN

  alias Fly.Postgres.LSN
  alias Fly.Postgres.FakeRepo

  describe "new/2" do
    test "returns :not_replicating source when lsn is nil" do
      %LSN{} = result = LSN.new(nil, :replay)
      assert result.source == :not_replicating
    end

    test "correctly parses string LSN value" do
      %LSN{} = result = LSN.new("0/17BB660", :replay)
      assert result.fpart == 0
      assert result.offset == 24_884_832

      %LSN{} = result = LSN.new("1/17BB660", :replay)
      assert result.fpart == 1
      assert result.offset == 24_884_832
    end

    test "raises exception when invalid format" do
      assert_raise ArgumentError, "invalid lsn format \"something-else\"", fn ->
        LSN.new("something-else", :replay)
      end
    end
  end

  describe "replicated?/2" do
    test "returns true when not replicating" do
      lsn_insert = %Fly.Postgres.LSN{fpart: 0, offset: 100_733_376, source: :insert}
      lsn_replay = %Fly.Postgres.LSN{fpart: nil, offset: nil, source: :not_replicating}
      assert true == LSN.replicated?(lsn_replay, lsn_insert)
    end

    test "returns true when they match" do
      lsn_insert = %Fly.Postgres.LSN{fpart: 0, offset: 100_733_376, source: :insert}
      lsn_replay = %Fly.Postgres.LSN{fpart: 0, offset: 100_733_376, source: :replay}
      assert true == LSN.replicated?(lsn_replay, lsn_insert)
    end

    test "returns true when replay is higher than the insert" do
      lsn_insert = %Fly.Postgres.LSN{fpart: 0, offset: 100_733_376, source: :insert}
      lsn_replay = %Fly.Postgres.LSN{fpart: 0, offset: 100_733_377, source: :replay}
      assert true == LSN.replicated?(lsn_replay, lsn_insert)
    end

    test "returns false when insert is higher than replay" do
      lsn_insert = %Fly.Postgres.LSN{fpart: 0, offset: 100_733_377, source: :insert}
      lsn_replay = %Fly.Postgres.LSN{fpart: 0, offset: 100_733_376, source: :replay}
      assert false == LSN.replicated?(lsn_replay, lsn_insert)
    end
  end

  describe "current_wal_insert/1" do
    test "returns correctly parsed LSN with :insert source" do
      FakeRepo.set_insert_lsn(%Fly.Postgres.LSN{fpart: 0, offset: 100_700_300, source: :insert})
      %LSN{source: :insert} = result = LSN.current_wal_insert(FakeRepo)
      assert result.fpart == 0
      assert result.offset == 100_700_300
    end
  end

  describe "last_wal_replay/1" do
    test "returns correctly parsed LSN with :replay source" do
      FakeRepo.set_replay_lsn(%Fly.Postgres.LSN{fpart: 0, offset: 100_700_300, source: :replay})
      %LSN{source: :replay} = result = LSN.last_wal_replay(FakeRepo)
      assert result.fpart == 0
      assert result.offset == 100_700_300
    end
  end
end
