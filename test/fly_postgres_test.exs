defmodule Fly.PostgresTest do
  # uses async false because we mess with the DATABASE_ENV value
  use ExUnit.Case, async: false

  doctest Fly.Postgres

  setup do
    System.put_env([{"FLY_REGION", "abc"}])
    System.put_env([{"PRIMARY_REGION", "xyz"}])

    System.put_env([
      {"DATABASE_URL",
       "postgres://some-user:some-pass@my-app-db.internal:5432/some_app?sslmode=disable"}
    ])

    %{}
  end

  describe "primary_db_url/0" do
    test "returns DATABASE_URL" do
      assert Fly.Postgres.primary_db_url() == System.get_env("DATABASE_URL")
    end
  end

  describe "replica_db_url/0" do
    test "changes port to 5433" do
      result = Fly.Postgres.replica_db_url()
      assert String.contains?(result, "5433")
    end

    test "includes current FLY_REGION in host name" do
      result = Fly.Postgres.replica_db_url()
      assert String.contains?(result, "@abc.my-app-db.internal")
    end
  end
end
