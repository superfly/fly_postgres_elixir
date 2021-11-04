defmodule FakeRepo do
  # A Fake Ecto.Repo that returns desired fake responses for testing.

  def query!("select CAST(pg_last_wal_replay_lsn() AS TEXT)") do

  end

  def query!("select CAST(pg_current_wal_insert_lsn() AS TEXT)") do

  end
end
