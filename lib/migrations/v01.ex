defmodule Fly.Postgres.Migrations.V01 do
  @moduledoc false
  # Approach inspired by Oban migrations: https://github.com/sorentwo/oban/blob/main/lib/oban/migrations/v04.ex
  use Ecto.Migration

  def up() do
    execute("""
    -- Function that takes an LSN value as the "last known value". The function
    -- loops and checks for that value to change. When the current DB value is
    -- newer than the one given, it returns the current DB value.
    --
    -- The from_lsn must come in as TEXT because "type `pg_lsn`
    -- can not be handled by the types module Postgrex.DefaultTypes".
    --
    -- The function also takes timer/duration for how long to loop and check. If
    -- no updates to the replicated LSNs happen in that time period, it returns
    -- the value `NULL` to communicate it timed out while waiting for a
    -- replication change, or "No Change".
    CREATE OR REPLACE FUNCTION watch_for_lsn_change(from_lsn TEXT, timeout REAL) RETURNS TEXT AS $$
    DECLARE
        elapsed REAL := 0;
        starting_lsn pg_lsn;
        current_lsn pg_lsn;
        difference DECIMAL;
    BEGIN
        LOOP
            -- Get the current WAL LSN and the target one.
            SELECT from_lsn::pg_lsn INTO starting_lsn;
            SELECT pg_last_wal_replay_lsn() INTO current_lsn;

            -- On first execution, there is no expected starting_lsn value so a NULL is sent.
            -- Immediately return the current_lsn value.
            IF starting_lsn IS NULL THEN
                RETURN CAST(current_lsn AS TEXT);
            END IF;

            -- starting_lsn has a value, get the difference. If still at the same value,
            -- then it will be 0. If at a newer value, it will be a positive number.
            difference = current_lsn - starting_lsn;

            -- If new value was detected, return the new value.
            IF difference > 0 THEN
                RETURN CAST(current_lsn AS TEXT);
            END IF;

            -- If we've timed out while waiting, return NULL
            IF elapsed > timeout THEN
                RETURN NULL;
            END IF;

            -- No change was detected. Wait for a moment. (50 msec)
            -- then loop again.
            PERFORM pg_sleep(0.05);
            elapsed = elapsed + 0.05;
        END LOOP;
      END;
    $$ LANGUAGE plpgsql;
    """)
  end

  def down() do
    execute("DROP FUNCTION IF EXISTS watch_for_lsn_change")
  end
end
