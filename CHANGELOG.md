# Changelog

## v0.1.10 (2021-11-04)

### Enhancements

- **Breaking change** - Added `rewrite_db_url` config option. Removes reliance on ENV setting MIX_ENV.

To use, in either `config/prod.exs` or `config/runtime.exs`, instruct the library to rewrite the `DATABASE_URL` used when connecting to the database. It takes into account which region is your primary region and attempts to connect to the primary or the replica accordingly.

```elixir
config :fly_postgres, rewrite_db_url: true
```
