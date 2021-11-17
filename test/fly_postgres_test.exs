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

    Application.put_env(:fly_postgres, :rewrite_db_url, true)

    %{}
  end

  describe "primary_db_url/0" do
    test "returns DATABASE_URL" do
      url = Fly.Postgres.primary_db_url()
      # Difference is that xyz is added to the host name to direct it to the primary region
      # The port shoudl stay as 5433
      assert url ==
               "postgres://some-user:some-pass@xyz.my-app-db.internal:5432/some_app?sslmode=disable"
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
