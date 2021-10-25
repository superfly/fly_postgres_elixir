defmodule Fly.Postgres.LSN do
  @moduledoc """
  Data structure that represents a PostgreSQL LSN or Log Sequence Number.

  Two LSN values can be compared using the `replicated?/2` function. An LSN
  associated with the DB modification has a `source` of `:insert`. On a replica
  instance, that can be used to see when the insert has been replicated locally.
  """
  alias __MODULE__

  defstruct fpart: nil, offset: nil, source: nil

  @type t :: %LSN{
          fpart: nil | integer,
          offset: nil | integer,
          source: :not_replicating | :insert | :replay
        }

  @doc """
  Create a new `Fly.Postgres.LSN` struct from the a queried WAL value.
  """
  def new(nil, :replay) do
    %LSN{fpart: nil, offset: nil, source: :not_replicating}
  end

  def new(lsn, source) when is_binary(lsn) and source in [:insert, :replay] do
    with [file_part_str, offset_str] <- String.split(lsn, "/"),
         {fpart, ""} = Integer.parse(file_part_str, 16),
         {offset, ""} = Integer.parse(offset_str, 16) do
      %LSN{fpart: fpart, offset: offset, source: source}
    else
      _ -> raise ArgumentError, "invalid lsn format #{inspect(lsn)}"
    end
  end

  # F1/O1 is at least as new as F2/O2 if (F1 > F2) or (F1 == F2 and O1 >= O2)
  @doc """
  Compare two `Fly.Postgres.LSN` structs to determine if the transaction representing a
  data change on the primary has been replayed locally.

  They are compared where the replay/replica value is in argument 1 and the
  insert value is in arguemnt two.

  ## Examples

      repo |> last_wal_replay() |> replicated?(primary_lsn)
  """
  def replicated?(replay_lsn, insert_lsn)
  def replicated?(%LSN{source: :not_replicating}, %LSN{source: :insert}), do: true

  def replicated?(%LSN{fpart: f1, offset: o1, source: :replay}, %LSN{
        fpart: f2,
        offset: o2,
        source: :insert
      }) do
    f1 > f2 or (f1 == f2 and o1 >= o2)
  end

  @doc """
  After performing a database modification, calling `current_wal_insert/1`
  returns a value that can be used to compare against a WAL value from the
  replica database to determine when the changes have been replayed on the
  replica.
  """
  def current_wal_insert(repo) do
    %Postgrex.Result{rows: [[lsn]]} =
      repo.query!("select CAST(pg_current_wal_insert_lsn() AS TEXT)")

    new(lsn, :insert)
  end

  @doc """
  When talking to a replica database, this returns a value for what changes have
  been replayed on the replica from the primary.
  """
  def last_wal_replay(repo) do
    %Postgrex.Result{rows: [[lsn]]} = repo.query!("select CAST(pg_last_wal_replay_lsn() AS TEXT)")
    new(lsn, :replay)
  end
end
