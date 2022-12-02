ExUnit.configure(capture_log: true)
ExUnit.start()
# Start the FakeRepo GenServer
{:ok, _pid} = Fly.Postgres.FakeRepo.start_link()
{:ok, _pid} = Fly.Postgres.LSN.Supervisor.start_link(repo: Fly.Postgres.FakeRepo)
