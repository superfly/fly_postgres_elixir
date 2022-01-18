defmodule Fly.PostgresTest do
  # uses async false because we mess with the DATABASE_ENV value
  use ExUnit.Case, async: false

  doctest Fly.Postgres

  @url "postgres://some-user:some-pass@top2.nearest.of.my-app-db.internal:5432/some_app"

  setup do
    System.put_env([{"FLY_REGION", "abc"}, {"PRIMARY_REGION", "xyz"}, {"DATABASE_URL", @url}])

    %{}
  end

  describe "replica_db_url/1" do
    test "changes port to 5433" do
      result = Fly.Postgres.replica_db_url(@url)
      assert String.contains?(result, "5433")
    end
  end

  describe "rewrite_database_url/1" do
    test "returns config unchanged when in primary region" do
      System.put_env([{"FLY_REGION", "xyz"}])
      config = [stuff: "THINGS", url: System.get_env("DATABASE_URL")]
      assert config == Fly.Postgres.rewrite_database_url(config)
    end

    test "changes port when not in primary region" do
      config = [stuff: "THINGS", url: System.get_env("DATABASE_URL")]
      # NOTE: Only the port number changed
      expected = "postgres://some-user:some-pass@top2.nearest.of.my-app-db.internal:5433/some_app"
      updated = Fly.Postgres.rewrite_database_url(config)
      assert expected == Keyword.get(updated, :url)
      # other things are altered
      assert Keyword.get(updated, :stuff) == Keyword.get(config, :stuff)
    end
  end

  describe "config_repo_url/1" do
    test "don't do anything when no :url is included" do
      config = [stuff: "THINGS", database: "my-db"]
      assert {:ok, config} == Fly.Postgres.config_repo_url(config)
    end

    test "update url for replica when given" do
      config = [stuff: "THINGS", url: System.get_env("DATABASE_URL")]
      # NOTE: Only the port number changed
      expected = "postgres://some-user:some-pass@top2.nearest.of.my-app-db.internal:5433/some_app"
      {:ok, updated} = Fly.Postgres.config_repo_url(config)
      assert expected == Keyword.get(updated, :url)
    end
  end
end
