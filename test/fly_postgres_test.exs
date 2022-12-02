defmodule Fly.PostgresTest do
  # uses async false because we mess with the DATABASE_ENV value
  use ExUnit.Case, async: false

  doctest Fly.Postgres

  alias Fly.Postgres.FakeRepo
  alias Fly.Postgres.LSN

  @url_dns "postgres://some-user:some-pass@top1.nearest.of.my-app-db.internal:5432/some_app"
  @url_base "postgres://some-user:some-pass@my-app-db.internal:5432/some_app"

  setup do
    System.put_env([{"FLY_REGION", "abc"}, {"PRIMARY_REGION", "xyz"}, {"DATABASE_URL", @url_dns}])

    %{}
  end

  describe "rewrite_database_url!/1" do
    test "returns config unchanged when in primary region and includes DNS helper parts" do
      System.put_env([{"FLY_REGION", "xyz"}])
      config = [stuff: "THINGS", url: System.get_env("DATABASE_URL")]
      assert config == Fly.Postgres.rewrite_database_url!(config)
    end

    test "adds DNS helper parts when missing from URL" do
      config = [stuff: "THINGS", url: @url_base]
      System.put_env([{"FLY_REGION", "xyz"}, {"DATABASE_URL", @url_base}])
      config = Fly.Postgres.rewrite_database_url!(config)
      assert Keyword.get(config, :url) |> String.contains?("top1.nearest.of.")
    end

    test "changes port when not in primary region" do
      config = [stuff: "THINGS", url: System.get_env("DATABASE_URL")]
      # NOTE: Only the port number changed
      expected = "postgres://some-user:some-pass@top1.nearest.of.my-app-db.internal:5433/some_app"
      updated = Fly.Postgres.rewrite_database_url!(config)
      assert expected == Keyword.get(updated, :url)
      # other things are altered
      assert Keyword.get(updated, :stuff) == Keyword.get(config, :stuff)
    end

    test "changes port and adds DNS helpers if missing when not in primary region" do
      config = [stuff: "THINGS", url: @url_base]
      # NOTE: Port number changed and DNS parts added to host
      expected = "postgres://some-user:some-pass@top1.nearest.of.my-app-db.internal:5433/some_app"
      updated = Fly.Postgres.rewrite_database_url!(config)
      assert Keyword.get(updated, :url) == expected
    end

    test "raises an exception when URL missing from config" do
      config = [
        stuff: "THINGS",
        database: "fluffly_prod",
        username: "fluff",
        password: "fluffy-nutter"
      ]

      assert_raise ArgumentError,
                   "Unable to rewrite database url in fly_postgres. No url was specified.",
                   fn ->
                     Fly.Postgres.rewrite_database_url!(config)
                   end
    end
  end

  describe "rewrite_host/1" do
    test "adds dns helpers if missing from host" do
      config = [stuff: "THINGS", url: @url_base]
      expected = "postgres://some-user:some-pass@top1.nearest.of.my-app-db.internal:5432/some_app"
      updated = Fly.Postgres.rewrite_host(config)
      assert Keyword.get(updated, :url) == expected
    end

    test "returns unmodified if dns top1 helper detected" do
      no_change =
        "postgres://some-user:some-pass@top1.nearest.of.my-app-db.internal:5432/some_app"

      config = [stuff: "THINGS", url: no_change]
      updated = Fly.Postgres.rewrite_host(config)
      assert Keyword.get(updated, :url) == no_change
    end

    test "changes to top1 if top2 is detected" do
      config = [
        stuff: "THINGS",
        url: "postgres://some-user:some-pass@top2.nearest.of.my-app-db.internal:5432/some_app"
      ]

      expected = "postgres://some-user:some-pass@top1.nearest.of.my-app-db.internal:5432/some_app"
      updated = Fly.Postgres.rewrite_host(config)
      assert Keyword.get(updated, :url) == expected
    end
  end

  describe "rewrite_replica_port/1" do
    test "if running on the primary, returns unchanged" do
      System.put_env([{"FLY_REGION", "xyz"}])
      # test when includes "top2.nearest.of..."
      config = [stuff: "THINGS", url: @url_dns]
      updated = Fly.Postgres.rewrite_replica_port(config)
      # NOTE: Port number NOT changed
      assert Keyword.get(updated, :url) == @url_dns
    end

    test "if not in primary region, change port to 5433" do
      System.put_env([{"FLY_REGION", "abc"}])
      # test when includes "top2.nearest.of..."
      config = [stuff: "THINGS", url: @url_dns]
      updated = Fly.Postgres.rewrite_replica_port(config)
      # NOTE: Port number should change
      expected = "postgres://some-user:some-pass@top1.nearest.of.my-app-db.internal:5433/some_app"
      assert Keyword.get(updated, :url) == expected
      assert @url_dns != expected
    end
  end

  describe "config_repo_url/1" do
    test "updates url with DNS for primary in prod env" do
      System.put_env([{"FLY_REGION", "xyz"}])
      config = [stuff: "THINGS", url: @url_base]
      # NOTE: Only the port number changed
      expected = "postgres://some-user:some-pass@top1.nearest.of.my-app-db.internal:5432/some_app"
      {:ok, updated} = Fly.Postgres.config_repo_url(config, :prod)
      assert expected == Keyword.get(updated, :url)
    end

    test "update url for replica PORT when given" do
      config = [stuff: "THINGS", url: System.get_env("DATABASE_URL")]
      # NOTE: Only the port number changed
      expected = "postgres://some-user:some-pass@top1.nearest.of.my-app-db.internal:5433/some_app"
      {:ok, updated} = Fly.Postgres.config_repo_url(config, :prod)
      assert expected == Keyword.get(updated, :url)
    end

    test "makes no changes in dev and test environments" do
      config = [stuff: "THINGS", database: "no-url"]
      assert {:ok, ^config} = Fly.Postgres.config_repo_url(config, :test)
      assert {:ok, ^config} = Fly.Postgres.config_repo_url(config, :dev)

      config = [stuff: "THINGS", url: @url_base]
      assert {:ok, ^config} = Fly.Postgres.config_repo_url(config, :test)
      assert {:ok, ^config} = Fly.Postgres.config_repo_url(config, :dev)
    end
  end

  def test_function() do
    "function result"
  end

  describe "__rpc_lsn__/4" do
    test "returns {:wal_lookup_failure, result} when WAL lookup query errors" do
      FakeRepo.set_insert_lsn(:raise_postgrex_error)

      assert {:wal_lookup_failure, "function result"} =
               Fly.Postgres.__rpc_lsn__(__MODULE__, :test_function, [], [])
    end

    test "returns the expected insert LSN and function result" do
      expected_lsn = LSN.new("0/0000000", :insert)
      FakeRepo.set_insert_lsn(expected_lsn)
      {lsn, "function result"} = Fly.Postgres.__rpc_lsn__(__MODULE__, :test_function, [], [])
      assert expected_lsn == lsn
    end
  end
end
