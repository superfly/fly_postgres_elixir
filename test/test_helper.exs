ExUnit.configure(capture_log: true)
ExUnit.start()
# Start the FakeRepo GenServer
{:ok, _pid} = Fly.Postgres.FakeRepo.start_link()
