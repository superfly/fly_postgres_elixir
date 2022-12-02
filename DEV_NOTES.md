# Development Notes

## Prep for Release

- Run `mix test`
- Run `mix format`

## Release

Hex.pm package: https://hex.pm/packages/fly_postgres

- Create a branch for the release.
- Update `mix.exs` version - [Version docs](https://hexdocs.pm/elixir/Version.html)
- Update `CHANGELOG.md`
- Push the PR branch to Github.
- Github: Merge the PR using "Squash and Merge". Delete branch.
- Check out `main` branch. Pull the merged PR.
- Tag the release: Ex: "v0.3.1" and push the tag.
- `mix hex.build`
- `mix hex.publish`
- Github release
  - https://github.com/superfly/fly_postgres_elixir/releases
  - Click "Draft a new release"
  - Choose the existing tag.
  - Click "Generate release notes". May update to remove the release prep PR.
  - Click "Publish release" button.


## Testing with development `fly_rpc`

In `mix.exs`, set the dep as:

```elixir
{:fly_rpc, git: "https://github.com/superfly/fly_rpc_elixir.git", branch: "dev-branch-name"},
```

Update:

- `mix deps.unlock fly_rpc`
- `mix deps.update fly_rpc`
- `mix test`

## Postgres features

Notes and documentation on the relevant Postgres features used by this library.

- `pg_current_wal_insert_lsn()`
  - Call for the new LSN after a modification. (Create, Update, Delete)
  - Will always return an LSN value.
- `pg_last_wal_replay_lsn()`
  - Call on a replica to get the most recent LSN that was replicated locally.
  - Returns `nil` on Primary DB, where it does not receive replication.
  - Returns an LSN value on replica DBs
- `pg_lsn` - Postgres datatype.
  - Not directly comparable. Can't use `<` or `>`.
  - Can subtract two LSN values and get the difference as a DOUBLE/float.
  - Can compare the difference. 0 = equal. Depending on which value was
    subtracted from which, the different (+/-) will mean different things.

Getting the values out in a query result requires casting to text. It's a binary result otherwise.

```elixir
Core.Repo.Local.query!("select CAST(pg_current_wal_insert_lsn() AS TEXT)")
Core.Repo.Local.query!("select CAST(pg_last_wal_replay_lsn() AS TEXT)")
```

Casting a text LSN value back into a `pg_lsn` type.

```sql
CAST('0/28B1A830' AS pg_lsn)
```

### Using the PG functions

This includes using the PG functions to make calls to the `watch_for_lsn_change`
stored procedure.

Waits 10 seconds before timing out. Result is `false`

```elixir
Core.Repo.Local.query!("SELECT watch_for_lsn_change('7/A25801C8', 10);")
```

Fetch the current LSN value as a string and test waiting for it. It is already there, so it results in an immediate return.

```elixir
%Postgrex.Result{rows: [[lsn]]} = Core.Repo.Local.query!("select CAST(pg_current_wal_insert_lsn() AS TEXT)")

Core.Repo.Local.query!("SELECT watch_for_lsn_change('#{lsn}', 10);")
```

Tests casting the replicated LSN value and attempts comparison. Returns `nil`. Not comparable.

```elixir
Core.Repo.Local.query!("SELECT pg_last_wal_replay_lsn() >= CAST('0/28B1A830' AS pg_lsn)")
```

**LEARNED:** The `pg_lsn` type supports subtraction, but does not support
comparison. Need to subtract one LSN from another and the examine the result.

**LEARNED:** `pg_last_wal_replay_lsn()` returns `nil` when called on the primary. Because it hasn't done any replication!

Perform subtraction to get the difference between two LSN values. The actual
numeric difference isn't important. Once returned as a DECIMAL/float, it can be
"compared" as `>` or `<`.

```elixir
Core.Repo.Local.query!("select pg_current_wal_insert_lsn() - '0/28B66750'::pg_lsn")
```

### Stored procedure testing

NOTE: The LSN value needs to be fetched from the DB and substituted. My
incrementing the value higher, it can be simulated that replication for it has
not happened yet and it will wait. Using the same value as the current LSN or a
lower value, it assumes the value has been replicated and returns immediately.

```elixir
lsn = "0/28B66730"
Core.Repo.Local.query!("SELECT watch_for_lsn_change('#{lsn}'::pg_lsn, 2);")
Core.Repo.Local.query!("SELECT watch_for_lsn_change(NULL, 2);")
Core.Repo.Local.query!("SELECT watch_for_lsn_change($1::pg_lsn, 2);", [nil])
```
