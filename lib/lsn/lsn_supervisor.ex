defmodule Core.LSN.Supervisor do
  # Automatically defines child_spec/1
  use Supervisor
  alias Fly.Postgres.LSN.Tracker
  alias Fly.Postgres.LSN.Reader

  def start_link(opts \\ []) do
    if !Keyword.has_key?(opts, :repo) do
      raise ArgumentError, ":repo is required when starting the LSN tracking processes"
    end

    name = Keyword.get(opts, :name, Core.LSN.Supervisor)
    base_name = Keyword.get(opts, :name, Core.LSN)
    Supervisor.start_link(__MODULE__, Keyword.put(opts, :base_name, base_name), name: name)
  end

  @impl true
  def init(opts) do
    repo_module = Keyword.get(opts, :repo)
    base_name = Keyword.get(opts, :base_name)

    children = [
      {Tracker, [repo: repo_module, base_name: base_name]},
      {Reader, [repo: repo_module, base_name: base_name]}
    ]

    # if the Tracker process dies, restart the reader
    Supervisor.init(children, strategy: :rest_for_one)
  end
end
