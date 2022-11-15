# Fly Postgres

Helps take advantage of geographically distributed Elixir applications using
Ecto and PostgreSQL in a primary/replica configuration on [Fly.io](https://fly.io).

[Online Documentation](https://hexdocs.pm/fly_postgres)

[Mark Ericksen's ElixirConf 2021 presentation](https://www.youtube.com/watch?v=IqnZnFpxLjI) explains more about what this library is for
and the problems it helps solve.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `fly_postgres` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:fly_postgres, "~> 0.2.0"}
  ]
end
```

Note that `fly_postgres` depends on `fly_rpc` so it is pulled in as well.
The configuration section below includes the relevant parts for `fly_rpc`.

## Configuration

### Repo

This assumes your project already has an `Ecto.Repo`. To start using the
`Fly.Repo`, here are the changes to make.

For a project named "MyApp", change it from this...

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Postgres
end
```

To something like this...

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

This renames your existing repo to "move it out of the way" and adds a new repo
to the same file. The new repo uses the `Fly.Repo` and links back to your
project's `Ecto.Repo`. The new repo has the same name as your original
`Ecto.Repo`, so your application will be referring to it now when talking to the
database.

The other change adds the `init` function to your `Ecto.Repo`. This
dynamically configures your `Ecto.Repo` to connect to the **primary** (writable)
database when your application is running in the primary region. When your
application is **not** in the primary region, it is configured to connect to the
nearest read-only replica. The replica is like a fast local cache of all your
data. The `.Local` idea is that your `Ecto.Repo` is configured to talk to it's
physically "local" database.

The `Fly.Repo` performs all **read** operations like `all`, `one`, and `get_by`
directly on the local replica. Other modifying functions like `insert`,
`update`, and `delete` are performed on the **primary database** through proxy
calls to a node in your Elixir cluster running in the primary region. That
ability is provided by the `fly_rpc` library.

The value of the `Mix.env()` is set at build time to `@env` and passed in to let
`fly_postgres` know about the project's build environment. `Fly.Postgres` only
attempts to rewrite the database URL when your app is running in `:prod` mode.
When running in `:dev` or `:test`, no `Ecto.Repo` configuration changes are
made.

### Migration Files

After changing your repo name, generating migrations can end up in the wrong place, or at least not where we want them.

You can override the inferred location in your config:

```elixir
config :my_app, MyApp.Repo.Local,
  priv: "priv/repo"
```

### Repo References

The goal with using this repo wrapper, is to leave the majority of your
application code and business logic unchanged. However, there are a few places
that need to be updated to make it work smoothly.

The following examples are places in your project code that need reference your
actual `Ecto.Repo`. Following the above example, it should point to
`MyApp.Repo.Local`.

- `test_helper.exs` files make references like this `Ecto.Adapters.SQL.Sandbox.mode(MyApp.Repo.Local, :manual)`
- `data_case.exs` files start the repo using `Ecto.Adapters.SQL.Sandbox.start_owner!` calls.
- `channel_case.exs` need to start your local repo.
- `conn_case.exs` need to start your local repo.
- `config/config.exs` needs to identify your local repo module. Ex: `ecto_repos: [MyApp.Repo.Local]`
- `config/dev.exs`, `config/test.exs`, `config/runtime.exs` - any special repo configuration should refer to your local repo.

With these project plumbing changes, you application code remains largely untouched!

### Primary Region

If your application is deployed to multiple Fly.io regions, the instances (or
nodes) must be clustered together.

Through ENV configuration, you tell the app which region is the "primary" region.

`fly.toml`

This example configuration says that the Sydney Australia region is the
"primary" region. This is where the primary postgres database is created and
where our application has fast write access to it.

```yaml
[env]
  PRIMARY_REGION = "syd"
```

### Application

There are two entries to add to your application supervision tree.

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    # ...

    children = [
      # Start the RPC server
      {Fly.RPC, []},
      # Start the Ecto repository
      MyApp.Repo.Local,
      # Start the supervisor for LSN tracking
      {Fly.Postgres.LSN.Supervisor, repo: MyApp.Repo.Local},
      #...
    ]

    # ...
  end
end
```

The following changes were made:

- Added the `Fly.RPC` GenServer
- Start your Repo
- Added `Fly.Postgres.LSN.Tracker` telling it which Repo to use.

### Multiple Ecto.Repos?

If you have multiple `Ecto.Repo`s setup in your application, you can still use `Fly.Postgres`. You will need an LSN Tracker for each repository that you want to work with.

In your application file, it would be similar to this:

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    # ...

    children = [
      # Start the RPC server
      {Fly.RPC, []},
      # Start Ecto repositories
      MyApp.Repo.Local_1,
      MyApp.Repo.Local_2,
      # Start the tracker after your DBs and name them.
      {Fly.Postgres.LSN.Tracker, repo: MyApp.Repo.Local_1, name: :repo_tracker_1},
      {Fly.Postgres.LSN.Tracker, repo: MyApp.Repo.Local_2, name: :repo_tracker_2},
      #...
    ]

    # ...
  end
end
```

### Stored Procedure

Create the stored procedure used to help monitor when Log Sequence Numbers (LSNs) change indicating data was replicated.

```
mix ecto.gen.migration add_fly_postgres_proc
```

```elixir
mix ecto.gen.migration add_fly_postgres_proc
defmodule MyApp.Repo.Local.Migrations.AddFlyPostgresProc do
  use Ecto.Migration

  def up do
    Fly.Postgres.Migrations.V01.up()
  end

  def down do
    Fly.Postgres.Migrations.V01.down()
  end
end

```

The stored procedure is only being executed when in remote (non-primary) regions. It actively watches for replication changes with the Postgres WAL (Write Ahead Log).

## Usage

### Local Development

If you get an error like `(ArgumentError) could not fetch environment variable "PRIMARY_REGION" because it is not set` then see the [README docs in `fly_rpc`](https://github.com/superfly/fly_rpc_elixir#local-development) for details on setting up your local development environment.

### Automatic Usage

Normal calls like `MyApp.Repo.all(User)` are performed on the local replica
repo. They are unchanged and work exactly as you'd expect.

Calls that _modify_ the database like "insert, update, and delete", are
performed through an RPC (Remote Procedure Call) in your application running in
the primary region.

In order for this to work, the application must be clustered together and
configured to identify which region is the "primary" region. Additionally, the
application needs to be deployed to multiple regions. This assumes an instance of the application is running in the primary region as well.

A call to `MyApp.Repo.insert(changeset)` is proxied to perform the insert
in the primary region. If the function is already running in the primary region,
it just executes normally locally. If the function is running in a non-primary
region, it makes a RPC execution to run on the primary.

The magic bits are that it additionally fetches the Postgres LSN (Log Sequence
Number) for the database after making the change. The calling function then
blocks, waits for the async database replication process to complete, and
continues on once the data modification has replayed on the local replica.

In this way, it becomes seamless for you and your code! You get the benefits of
being globally distributed and running closer to your users without re-designing your application!

By default, a Repo function that modifies the database is proxied to a primary server
and waits for the data to be replicated locally before continuing. Passing
the `await: false` option instructs the proxy code to not wait for replication to complete.

This is helpful when you only need the function result or the data is not
immediately needed locally.

```elixir
MyApp.Repo.insert(changeset, await: false)

MyApp.Repo.update(changeset, await: false)

MyApp.Repo.delete(item, await: false)
```

### Explicit RPC Usage

When business logic code makes a number of changes or does some back and forth
with the database, the "Automatic Usage" is too slow. An example is looping
through a list and performing a database insert on each iteration. Waiting for
each insert to complete and be locally replicated before performing the next
iteration could be very slow!

For those cases, execute the function that does all the database work but do it
in the primary region where it is physically close to the database.

```elixir
Fly.Postgres.rpc_and_wait(MyModule, :do_complex_work, [arg1, arg2])
```

The function is executed in the primary region and locally, blocks until
any relevant database changes are replicated.

### Explicit RPC but don't Wait for Replication

Sometimes you might not want to wait for DB replication. Perhaps it's a
fire-and-forget or the function result is enough.

For this case, you can use the `fly_rpc` library directly.

```elixir
Fly.rpc_primary(MyModule, :do_work, [arg1, arg2])
```

This is a convenience function which is equivalent to the following:

```elixir
Fly.RPC.rpc_region(:primary, MyModule, :do_work, [arg1, arg2])
```

This also works when modifying the database too.

### Using Ecto Queries in Migrations

If you are trying to run an Ecto Query in a Migration, it will fail when using `MyApp.Repo.insert(...)` or `MyApp.Repo.update(...)`.

The solution is to explicitly use the local repo instead.

`MyApp.Repo.Local.insert(...)` or `MyApp.Repo.Local.update(...)`

**Explanation:**

It doesn't work to use the repo wrapper in a migration because the Tracker started in your `MyApp.Application` hasn't been started. When running migrations, the "Application" is not started because we could have GenServers that make queries and interact with the database. When running migrations, those parts of the application shouldn't be running because the very structure of the database can change.

It is safe to use `MyApp.Repo.Local` because on Fly.io, migrations are run in the primary region that already has direct access to the writable database.

In general, it is discouraged to use `.update(...)`, `.insert(...)`, and `.delete(...)` statements in migrations. For more information on why that presents problems and alternative options, check out [this section](https://fly.io/phoenix-files/backfilling-data/) of the [Safe Ecto Migrations](https://fly.io/phoenix-files/safe-ecto-migrations/) series.

## Production Environment

### Prevent temporary outages during deployments

When deploying on [Fly.io](https://fly.io), a new instance is rolled out before removing the old instance. This creates a period of time where both new and old instances are deployed together. By default, when deploying a Phoenix application, a new BEAM cookie is generated for each deployment. When the new instance rolls out with a new BEAM cookie, the old and new instances will not cluster together. BEAM instances must have the same cookie in order to connect. This is by design.

This means a newly deployed application running in a secondary region using [fly_postgres](https://github.com/superfly/fly_postgres_elixir) is unable to perform writes to the older application running in the primary region. It is possible for writes to fail during that rollout window.

To prevent this problem, the BEAM cookie can be explicitly set instead of using a randomly generated one for new builds. When explicitly set, the newly deployed application is still able to connect and cluster with the older application running in the primary region.

Here is a guide to setting a static cookie for your project that is written into the code itself. This is fine to do because the cookie isn't considered a secret used for security.

[fly.io/docs/app-guides/elixir-static-cookie/](https://fly.io/docs/app-guides/elixir-static-cookie/)

When the cookie is static and unchanged from one deployment to the next, then applications can continue to cluster and access the applications running in primary region.

## LSN Polling

The library polls the local database for what point in the replication process
it has gotten to. It uses the LSN (Log Sequence Number) to determine that. Using
this information, a process making changes against the primary database can
request to be notified once the LSN it cares about has been replicated. This
enables blocking operations that pause and wait for replication to complete.

The active polling only happens once a process has requested to be notified.
When there are no pending requests, there is no active polling. Also, there can
be many active pending requests and still there will be only 1 polling process.
So each waiting process isn't polling the database itself.

The polling design scales well and doesn't perform work when there is nothing to
track.

## Backup Regions

By default, Fly.io defines some "backup regions" where your app may be deployed.
This can happen even during a normal deploy where the app instance created for
running database migrations could come up in a backup region.

The `fly_postgres` library makes an important assumption: that the app instance
is always running in a region with either the primary or replica database.

To make your deployments reliably end up in a desired region, we'll disable the
backup regions. Or rather, explicitly set which regions to use as backup
regions.

To see the current set of backup regions:

```shell
fly regions backup list
```

If we want to serve 2 regions like `lax` and `syd`, then we can set the backup regions like this:

```shell
fly regions backup lax syd
```

This makes the backup regions only use the desired regions.

## Ensuring a Deployment in your Primary Region

Currently it is possible to be configured for two regions, scale your app to 2
instances and end up with both app instances in the **same region**! Clearly,
when one app needs to be running in the primary region in order to receive the
RPC calls, it's really important that it consistently be there!

A Fly.io config option lets us have more control over that.

```shell
fly scale count 2 --max-per-region 1
```

Using the `--max-per-region 1` ensures each region will not get an unbalanced
number like 2 in one place.

If your scale count matches up with your desired number of regions, then they
will be evenly distributed.
