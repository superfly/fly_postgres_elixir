ExUnit.configure(capture_log: true)
ExUnit.start()
# Start the FakeRepo GenServer
{:ok, _pid} = Fly.Postgres.FakeRepo.start_link()
# Set to use the FakeRepo
Application.put_env(:fly_postgres, :local_repo, Fly.Postgres.FakeRepo)
