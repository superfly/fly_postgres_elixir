# Changelog

## v0.1.13 (2021-12-28)

- Improvement: Use new Fly.io internal DNS feature to connect to the "nearest" replica database when deployed to a region other than the primary region.

## v0.1.12 (2021-11-16)

- Fix: For dev and test environments, return `nil` for customized DATABASE_URL setting. This removes requirement for setting the DATABASE_URL in an ENV for local dev and test. Additionally, this fixes an issue where the dev database was being used when running tests.
- Added usage documentation in the README under "Config Files"
- New usage documentation section in README titled "Releases and Migrations"

## v0.1.11 (2021-11-06)

- Enhancement: Internally, improved logic around polling the database for the replication status. Only polls when there is a request for replication notification.

## v0.1.10 (2021-11-04)

### Enhancements

- **Breaking change** - Added `rewrite_db_url` config option. Removes reliance on ENV setting MIX_ENV.

To use, in either `config/prod.exs` or `config/runtime.exs`, instruct the library to rewrite the `DATABASE_URL` used when connecting to the database. It takes into account which region is your primary region and attempts to connect to the primary or the replica accordingly.

```elixir
config :fly_postgres, rewrite_db_url: true
```
