defmodule Fly.PostgresTest do
  # uses async false because we mess with the DATABASE_ENV value
  use ExUnit.Case, async: false

  doctest Fly.Postgres

  setup do
    System.put_env([{"FLY_REGION", "abc"}])
    System.put_env([{"PRIMARY_REGION", "xyz"}])

    System.put_env([
      {"DATABASE_URL",
       "postgres://some-user:some-pass@top2.nearest.of.my-app-db.internal:5432/some_app"}
    ])

    Application.put_env(:fly_postgres, :rewrite_db_url, true)

    %{}
  end

  describe "primary_db_url/0" do
    test "returns DATABASE_URL" do
      url = Fly.Postgres.primary_db_url()
      # Difference is that xyz is added to the host name to direct it to the primary region
      # The port shoudl stay as 5433
      assert url ==
               "postgres://some-user:some-pass@top2.nearest.of.my-app-db.internal:5432/some_app"
    end
  end

  describe "replica_db_url/0" do
    test "changes port to 5433" do
      result = Fly.Postgres.replica_db_url()
      assert String.contains?(result, "5433")
    end

    test "includes current FLY_REGION in host name" do
      result = Fly.Postgres.replica_db_url()
      assert String.contains?(result, "@top2.nearest.of.my-app-db.internal")
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
