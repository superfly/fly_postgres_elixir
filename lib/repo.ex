defmodule Fly.Repo do
  @moduledoc """
  This wraps the built-in `Ecto.Repo` functions to proxy writable functions like
  insert, update and delete to be performed on the an Elixir node in the primary
  region.

  To use it, rename your existing repo module and add a new module with the same
  name as your original repo like this.


  Original code:

  ```elixir
  defmodule MyApp.Repo do
    use Ecto.Repo,
      otp_app: :my_app,
      adapter: Ecto.Adapters.Postgres
  end
  ```

  Changes to:

  ```elixir
  defmodule MyApp.Repo.Local do
    use Ecto.Repo,
      otp_app: :my_app,
      adapter: Ecto.Adapters.Postgres

    # Dynamically configure the database url based for runtime environment.
    def init(_type, config) do
      {:ok, Keyword.put(config, :url, Fly.Postgres.database_url())}
    end
  end

  defmodule Core.Repo do
    use Fly.Repo, local_repo: MyApp.Repo.Local
  end
  ```

  Using the same name allows your existing code to seamlessly work with the new
  repo.

  When explicitly managing database transactions like using Multi or
  `start_transaction`, when used to modify data, those functions should be
  called by an RPC so they run in the primary region.

  ```elixir
  Fly.RPC.rpc_region(:primary, MyModule, :my_function_that_uses_multi, [my,
  args], opts)
  ```
  """

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @local_repo Keyword.fetch!(opts, :local_repo)
      @timeout Keyword.get(opts, :timeout, 5_000)
      @replication_timeout Keyword.get(opts, :replication_timeout, 5_000)

      # Here we are injecting as little as possible then calling out to the
      # library functions.

      @doc """
      See `Ecto.Repo.config/0` for full documentation.
      """
      @spec config() :: Keyword.t()
      def config() do
        @local_repo.config()
      end

      @doc """
      Calculate the given `aggregate`.

      See `Ecto.Repo.aggregate/3` for full documentation.
      """
      def aggregate(queryable, aggregate, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:aggregate, [queryable, aggregate, opts])
      end

      @doc """
      Calculate the given `aggregate` over the given `field`.

      See `Ecto.Repo.aggregate/4` for full documentation.
      """
      def aggregate(queryable, aggregate, field, opts) do
        unquote(__MODULE__).__exec_local__(:aggregate, [queryable, aggregate, field, opts])
      end

      @doc """
      Fetches all entries from the data store matching the given query.

      See `Ecto.Repo.all/2` for full documentation.
      """
      def all(queryable, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:all, [queryable, opts])
      end

      @doc """
      Deletes a struct using its primary key.

      See `Ecto.Repo.delete/2` for full documentation.
      """
      def delete(struct_or_changeset, opts \\ []) do
        unquote(__MODULE__).__exec_on_primary__(:delete, [struct_or_changeset, opts], opts)
      end

      @doc """
      Same as `delete/2` but returns the struct or raises if the changeset is invalid.

      See `Ecto.Repo.delete!/2` for full documentation.
      """
      def delete!(struct_or_changeset, opts \\ []) do
        unquote(__MODULE__).__exec_on_primary__(:delete!, [struct_or_changeset, opts], opts)
      end

      @doc """
      Deletes all entries matching the given query.

      See `Ecto.Repo.delete_all/2` for full documentation.
      """
      def delete_all(queryable, opts \\ []) do
        unquote(__MODULE__).__exec_on_primary__(:delete_all, [queryable, opts], opts)
      end

      @doc """
      Checks if there exists an entry that matches the given query.

      See `Ecto.Repo.exists?/2` for full documentation.
      """
      def exists?(queryable, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:exists?, [queryable, opts])
      end

      @doc """
      Fetches a single struct from the data store where the primary key matches the given id.

      See `Ecto.Repo.get/3` for full documentation.
      """
      def get(queryable, id, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:get, [queryable, id, opts])
      end

      @doc """
      Similar to `get/3` but raises `Ecto.NoResultsError` if no record was found.

      See `Ecto.Repo.get!/3` for full documentation.
      """
      def get!(queryable, id, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:get!, [queryable, id, opts])
      end

      @doc """
      Fetches a single result from the query.

      See `Ecto.Repo.get_by/3` for full documentation.
      """
      def get_by(queryable, clauses, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:get_by, [queryable, clauses, opts])
      end

      @doc """
      Similar to `get_by/3` but raises `Ecto.NoResultsError` if no record was found.

      See `Ecto.Repo.get_by!/3` for full documentation.
      """
      def get_by!(queryable, clauses, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:get_by!, [queryable, clauses, opts])
      end

      @doc """
      Inserts a struct defined via Ecto.Schema or a changeset.

      See `Ecto.Repo.insert/2` for full documentation.
      """
      def insert(struct_or_changeset, opts \\ []) do
        unquote(__MODULE__).__exec_on_primary__(:insert, [struct_or_changeset, opts], opts)
      end

      @doc """
      Same as `insert/2` but returns the struct or raises if the changeset is invalid.

      See `Ecto.Repo.insert!/2` for full documentation.
      """
      def insert!(struct_or_changeset, opts \\ []) do
        unquote(__MODULE__).__exec_on_primary__(:insert!, [struct_or_changeset, opts], opts)
      end

      @doc """
      Inserts all entries into the repository.

      See `Ecto.Repo.insert_all/3` for full documentation.
      """
      def insert_all(schema_or_source, entries_or_query, opts \\ []) do
        unquote(__MODULE__).__exec_on_primary__(
          :insert_all,
          [
            schema_or_source,
            entries_or_query,
            opts
          ],
          opts
        )
      end

      @doc """
      Inserts or updates a changeset depending on whether the struct is persisted or not

      See `Ecto.Repo.insert_or_update/2` for full documentation.
      """
      def insert_or_update(changeset, opts \\ []) do
        unquote(__MODULE__).__exec_on_primary__(:insert_or_update, [changeset, opts], opts)
      end

      @doc """
      Same as `insert_or_update!/2` but returns the struct or raises if the changeset is invalid.

      See `Ecto.Repo.insert_or_update!/2` for full documentation.
      """
      def insert_or_update!(changeset, opts \\ []) do
        unquote(__MODULE__).__exec_on_primary__(:insert_or_update!, [changeset, opts], opts)
      end

      @doc """
      Fetches a single result from the query.

      See `Ecto.Repo.one/2` for full documentation.
      """
      def one(queryable, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:one, [queryable, opts])
      end

      @doc """
      Similar to a `one/2` but raises Ecto.NoResultsError if no record was found.

      See `Ecto.Repo.one!/2` for full documentation.
      """
      def one!(queryable, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:one!, [queryable, opts])
      end

      @doc """
      Preloads all associations on the given struct or structs.

      See `Ecto.Repo.preload/3` for full documentation.
      """
      def preload(structs_or_struct_or_nil, preloads, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:preload, [
          structs_or_struct_or_nil,
          preloads,
          opts
        ])
      end

      @doc """
      A user customizable callback invoked for query-based operations.

      See `Ecto.Repo.preload/3` for full documentation.
      """
      def prepare_query(operation, query, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:prepare_query, [operation, query, opts])
      end

      @doc """
      Reloads a given schema or schema list from the database.

      See `Ecto.Repo.reload/2` for full documentation.
      """
      def reload(struct_or_structs, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:reload, [struct_or_structs, opts])
      end

      @doc """
      Similar to `reload/2`, but raises when something is not found.

      See `Ecto.Repo.reload!/2` for full documentation.
      """
      def reload!(struct_or_structs, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:reload, [struct_or_structs, opts])
      end

      @doc """
      Rolls back the current transaction.

      Defaults to the primary database repo. Assumes the transaction was used for
      data modification.

      See `Ecto.Repo.rollback/1` for full documentation.
      """
      def rollback(value) do
        unquote(__MODULE__).__exec_local__(:rollback, [value])
      end

      @doc """
      Returns a lazy enumerable that emits all entries from the data store matching the given query.

      See `Ecto.Repo.stream/2` for full documentation.
      """
      def stream(queryable, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:stream, [queryable, opts])
      end

      @doc """
      Runs the given function or Ecto.Multi inside a transaction.

      This defaults to the primary (writable) repo as it is assumed this is being
      used for data modification. Override to operate on the replica.

      See `Ecto.Repo.transaction/2` for full documentation.
      """
      def transaction(fun_or_multi, opts \\ []) do
        unquote(__MODULE__).__exec_local__(:transaction, [fun_or_multi, opts])
      end

      @doc """
      Updates a changeset using its primary key.

      See `Ecto.Repo.update/2` for full documentation.
      """
      def update(changeset, opts \\ []) do
        unquote(__MODULE__).__exec_on_primary__(:update, [changeset, opts], opts)
      end

      @doc """
      Same as `update/2` but returns the struct or raises if the changeset is invalid.

      See `Ecto.Repo.update!/2` for full documentation.
      """
      def update!(changeset, opts \\ []) do
        unquote(__MODULE__).__exec_on_primary__(:update!, [changeset, opts], opts)
      end

      @doc """
      Updates all entries matching the given query with the given values.

      See `Ecto.Repo.update_all/3` for full documentation.
      """
      def update_all(queryable, updates, opts \\ []) do
        unquote(__MODULE__).__exec_on_primary__(:update_all, [queryable, updates, opts], opts)
      end

      def __exec_local__(func, args) do
        apply(@local_repo, func, args)
      end

      def __exec_on_primary__(func, args, opts) do
        # Default behavior is to wait for replication. If `:await` is set to
        # false/falsey then skip the LSN query and waiting for replication.
        if Keyword.get(opts, :await, true) do
          rpc_timeout = Keyword.get(opts, :rpc_timeout, @timeout)
          replication_timeout = Keyword.get(opts, :replication_timeout, @replication_timeout)

          Fly.Postgres.rpc_and_wait(@local_repo, func, args,
            rpc_timeout: rpc_timeout,
            replication_timeout: replication_timeout,
            tracker: Keyword.get(opts, :tracker)
          )
        else
          Fly.rpc_primary(@local_repo, func, args, timeout: @timeout)
        end
      end
    end
  end
end
