# Changelog

## v0.2.6 (2022-08-17)

Fixed `reload!` wrapped function being incorrectly mapped to `reload` (missing the "!"). Merged [PR #26](https://github.com/superfly/fly_postgres_elixir/pull/26)

## v0.2.5 (2022-03-03)

Removed default value for `Fly.Postgres.config_repo_url/2` that was always defaulting to `:prod` when used by other apps.

## v0.2.4 (2022-03-03)

* Only rewrite DB URLs in prod builds by @brainlid in https://github.com/superfly/fly_postgres_elixir/pull/24

Creates a breaking change. Projects will not compile until updated. The update is simple.

In the wrapped repo file, use the following as a template.

```elixir
defmodule MyApp.Repo.Local do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Postgres

  @env Mix.env()

  # Dynamically configure the database url based on runtime and build
  # environments.
  def init(_type, config) do
    Fly.Postgres.config_repo_url(config, @env)
  end
end

defmodule MyApp.Repo do
  use Fly.Repo, local_repo: MyApp.Repo.Local
end
```

This stores the build environment and passes that into the `Fly.Postgres.config_repo_url/2` function. The function changed to not attempt any URL rewriting when in `:dev` or `:test` modes. It only looks to change the URL when in `:prod` mode.

See [PR #24](https://github.com/superfly/fly_postgres_elixir/pull/24) for more information.

## v0.2.3 (2022-03-01)

* convert "top2" to "top1" for host url by @brainlid in https://github.com/superfly/fly_postgres_elixir/pull/23

## v0.2.2 (2022-02-28)

- Improvement: The repo wraps a few more functions that are needed for the Oban library to work smoothly. Contribution by @sorentwo in https://github.com/superfly/fly_postgres_elixir/pull/22

## v0.2.1 (2022-02-02)

- Improvement: Support passing `replication_timeout` config in more places.

```elixir
defmodule MyApp.Repo do
  use Fly.Repo, local_repo: MyApp.Repo.Local, replication_timeout: 5_000
end

# or

MyApp.Repo.insert(changeset, replication_timeout: 5_000)
```

## v0.2.0 (2022-02-01)

The jump to a 0.2 indicates a significant change in the configuration of the library. Please review the README under the Configuration section.

- Breaking Change: Timeout configuration is passed in through keyword lists now, where previously it was a stand-alone argument. This is only an issue if you were explicitly running functions on the primary and you specified a timeout. For most uses, it happens invisibly.

Note that 2 different timeouts can be specified.

  - `:rpc_timeout` - The duration to allow for the RPC function to execute before timing out. Default is 5 seconds.
  - `:replication_timeout` - The duration to allow for the tracked LSN value to replicated to the local replica before timing out. Default is 5 seconds.

- Breaking Change: In your Application file where the `Fly.Postgres.LSN.Tracker` is added to a supervisor, the configuration changed. It requires passing the linked repo in as an argument.

```
{Fly.Postgres.LSN.Tracker, repo: MyApp.Repo.Local},
```

- Improvement: Supports multiple Ecto.Repos in a project. When using multiple, an LSN.Tracker is setup for each repo. You must specify which tracker is being waited on for synchronization.
- Improvement: Removed need for extra config in `config.exs` linking a single repo to the `fly_postgres` library.
- Improvement: Added instructions to `README.md` for configuring `Ecto.Repo` migration locations. Once your repo is renamed, generating a new migration can infer a location you didn't want.

## v0.1.13 (2021-12-28)

- Improvement: Use new Fly.io internal DNS feature to connect to the "nearest" replica database when deployed to a region other than the primary region.

Solves a problem when an instance is deployed to a backup region where the replica isn't deployed. Also helps make more resilient in going to closest replica it can find should any other problems prevent the expected one to be available.

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
