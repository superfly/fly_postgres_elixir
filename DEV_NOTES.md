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
- Tag the release: Ex: "v0.2.6" and push the tag.
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
