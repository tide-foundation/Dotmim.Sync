using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// Abstract class for all database scope info  builders.
    /// </summary>
    public abstract class DbScopeBuilder
    {

        // Internal commands cache
        private ConcurrentDictionary<string, Lazy<SyncPreparedCommand>> commands = new();

        /// <summary>
        /// Gets the parsed name of the table.
        /// </summary>
        public abstract DbTableNames GetParsedScopeInfoTableNames();

        /// <summary>
        /// Gets the parsed name of the table.
        /// </summary>
        public abstract DbTableNames GetParsedScopeInfoClientTableNames();

        /// <summary>
        /// Returns a command to check if the scope_info table exists.
        /// </summary>
        public abstract DbCommand GetExistsScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if the scope_info_client table exists.
        /// </summary>
        public abstract DbCommand GetExistsScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to create the scope_info table.
        /// </summary>
        public abstract DbCommand GetCreateScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to create the scope_info_client table.
        /// </summary>
        public abstract DbCommand GetCreateScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to get all scope info.
        /// </summary>
        public abstract DbCommand GetAllScopeInfosCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to get all scope info clients.
        /// </summary>
        public abstract DbCommand GetAllScopeInfoClientsCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to get a scope info.
        /// </summary>
        public abstract DbCommand GetScopeInfoCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to get a scope info client.
        /// </summary>
        public abstract DbCommand GetScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to insert a new scope info.
        /// </summary>
        public abstract DbCommand GetInsertScopeInfoCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to insert a new scope info client.
        /// </summary>
        public abstract DbCommand GetInsertScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to delete a scope info.
        /// </summary>
        public abstract DbCommand GetDeleteScopeInfoCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to delete a scope info client.
        /// </summary>
        public abstract DbCommand GetDeleteScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to update a scope info.
        /// </summary>
        public abstract DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to update a scope info client.
        /// </summary>
        public abstract DbCommand GetUpdateScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if a scope info exists.
        /// </summary>
        public abstract DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to drop the scope_info table.
        /// </summary>
        public abstract DbCommand GetDropScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to drop the scope_info_client table.
        /// </summary>
        public abstract DbCommand GetDropScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if a scope info exists.
        /// </summary>
        public abstract DbCommand GetExistsScopeInfoCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if a scope info client exists.
        /// </summary>
        public abstract DbCommand GetExistsScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        // -----------------------------------------------------------------
        // Tide fork (atomic-errors-batch): per-row, per-scope errors-batch table.
        //
        // Default implementations return null so existing providers that have not
        // been migrated continue to work — the orchestrator detects the null and
        // falls back to the legacy file-based BatchInfo path. The SQLite provider
        // overrides these to enable the atomic in-DB errors batch.
        // -----------------------------------------------------------------

        /// <summary>
        /// Tide fork: returns a command to check if the per-row errors table exists.
        /// Default null => provider does not support the feature.
        /// </summary>
        public virtual DbCommand GetExistsScopeInfoClientErrorsTableCommand(DbConnection connection, DbTransaction transaction) => null;

        /// <summary>
        /// Tide fork: returns a command to create the per-row errors table.
        /// Default null => provider does not support the feature.
        /// </summary>
        public virtual DbCommand GetCreateScopeInfoClientErrorsTableCommand(DbConnection connection, DbTransaction transaction) => null;

        /// <summary>
        /// Tide fork: returns a command to drop the per-row errors table.
        /// Default null => provider does not support the feature.
        /// </summary>
        public virtual DbCommand GetDropScopeInfoClientErrorsTableCommand(DbConnection connection, DbTransaction transaction) => null;

        /// <summary>
        /// Tide fork: returns a command to SELECT all rows from the per-row errors
        /// table for a given scope name. Parameter: <c>sync_scope_name</c>.
        /// </summary>
        public virtual DbCommand GetLoadScopeInfoClientErrorRowsCommand(DbConnection connection, DbTransaction transaction) => null;

        /// <summary>
        /// Tide fork: returns a command to upsert a single failed row. Parameters:
        /// <c>sync_scope_name</c>, <c>table_name</c>, <c>table_schema</c>,
        /// <c>row_state</c>, <c>primary_key_json</c>, <c>row_payload_json</c>,
        /// <c>created_at_ticks</c>.
        /// </summary>
        public virtual DbCommand GetSaveScopeInfoClientErrorRowCommand(DbConnection connection, DbTransaction transaction) => null;

        /// <summary>
        /// Tide fork: returns a command to delete a single failed row by
        /// (scope, table, schema, pk-json). Used by the clean-errors path.
        /// </summary>
        public virtual DbCommand GetDeleteScopeInfoClientErrorRowCommand(DbConnection connection, DbTransaction transaction) => null;

        /// <summary>
        /// Tide fork: returns a command to delete every row in the per-row errors
        /// table for a given scope name. Parameter: <c>sync_scope_name</c>.
        /// </summary>
        public virtual DbCommand GetDeleteScopeInfoClientErrorRowsForScopeCommand(DbConnection connection, DbTransaction transaction) => null;

        /// <summary>
        /// Remove a Command from internal shared dictionary.
        /// </summary>
        internal void RemoveCommands() => this.commands.Clear();

        /// <summary>
        /// Tide fork (atomic-errors-batch): classifies the per-row errors-batch
        /// commands so the dispatch layer can return null (=> "feature unsupported
        /// by this provider") instead of throwing MissingCommandException.
        /// </summary>
        private static bool IsTideErrorsBatchCommand(DbScopeCommandType commandType) => commandType is
            DbScopeCommandType.ExistsScopeInfoClientErrorsTable
            or DbScopeCommandType.CreateScopeInfoClientErrorsTable
            or DbScopeCommandType.DropScopeInfoClientErrorsTable
            or DbScopeCommandType.LoadScopeInfoClientErrorRows
            or DbScopeCommandType.SaveScopeInfoClientErrorRow
            or DbScopeCommandType.DeleteScopeInfoClientErrorRow
            or DbScopeCommandType.DeleteScopeInfoClientErrorRowsForScope;

        /// <summary>
        /// Get the command from provider, check connection is opened, affect connection and transaction
        /// Prepare the command parameters and add scope parameters.
        /// </summary>
        internal DbCommand GetCommandAsync(DbScopeCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            var scopeInfoTableNames = this.GetParsedScopeInfoTableNames();

            // Create the key
            var commandKey = $"{connection.DataSource}-{connection.Database}-{scopeInfoTableNames.NormalizedName}-{commandType}";

            var command = commandType switch
            {
                DbScopeCommandType.GetAllScopeInfos => this.GetAllScopeInfosCommand(connection, transaction),
                DbScopeCommandType.GetAllScopeInfoClients => this.GetAllScopeInfoClientsCommand(connection, transaction),

                DbScopeCommandType.GetScopeInfo => this.GetScopeInfoCommand(connection, transaction),
                DbScopeCommandType.GetScopeInfoClient => this.GetScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.CreateScopeInfoTable => this.GetCreateScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.CreateScopeInfoClientTable => this.GetCreateScopeInfoClientTableCommand(connection, transaction),

                DbScopeCommandType.ExistsScopeInfoTable => this.GetExistsScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.ExistsScopeInfoClientTable => this.GetExistsScopeInfoClientTableCommand(connection, transaction),

                DbScopeCommandType.InsertScopeInfo => this.GetInsertScopeInfoCommand(connection, transaction),
                DbScopeCommandType.InsertScopeInfoClient => this.GetInsertScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.UpdateScopeInfo => this.GetUpdateScopeInfoCommand(connection, transaction),
                DbScopeCommandType.UpdateScopeInfoClient => this.GetUpdateScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.DeleteScopeInfo => this.GetDeleteScopeInfoCommand(connection, transaction),
                DbScopeCommandType.DeleteScopeInfoClient => this.GetDeleteScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.ExistScopeInfo => this.GetExistsScopeInfoCommand(connection, transaction),
                DbScopeCommandType.ExistScopeInfoClient => this.GetExistsScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.DropScopeInfoTable => this.GetDropScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.DropScopeInfoClientTable => this.GetDropScopeInfoClientTableCommand(connection, transaction),

                DbScopeCommandType.GetLocalTimestamp => this.GetLocalTimestampCommand(connection, transaction),

                // Tide fork (atomic-errors-batch): per-row errors table commands.
                DbScopeCommandType.ExistsScopeInfoClientErrorsTable => this.GetExistsScopeInfoClientErrorsTableCommand(connection, transaction),
                DbScopeCommandType.CreateScopeInfoClientErrorsTable => this.GetCreateScopeInfoClientErrorsTableCommand(connection, transaction),
                DbScopeCommandType.DropScopeInfoClientErrorsTable => this.GetDropScopeInfoClientErrorsTableCommand(connection, transaction),
                DbScopeCommandType.LoadScopeInfoClientErrorRows => this.GetLoadScopeInfoClientErrorRowsCommand(connection, transaction),
                DbScopeCommandType.SaveScopeInfoClientErrorRow => this.GetSaveScopeInfoClientErrorRowCommand(connection, transaction),
                DbScopeCommandType.DeleteScopeInfoClientErrorRow => this.GetDeleteScopeInfoClientErrorRowCommand(connection, transaction),
                DbScopeCommandType.DeleteScopeInfoClientErrorRowsForScope => this.GetDeleteScopeInfoClientErrorRowsForScopeCommand(connection, transaction),

                _ => throw new Exception($"This DbScopeCommandType {commandType} not exists"),
            };

            // Tide fork (atomic-errors-batch): the per-row errors-batch commands
            // are optional — providers that have not been migrated return null
            // from the virtual factory and the orchestrator falls back to the
            // legacy file-based path. Surface that absence to the caller as a
            // null command rather than throwing, so the existing
            // MissingCommandException semantics for the legacy commands are
            // preserved while the new commands stay best-effort.
            if (command == null && IsTideErrorsBatchCommand(commandType))
                return null;

            if (command == null)
                throw new MissingCommandException(commandType.ToString());

            if (connection == null)
                throw new MissingConnectionException();

            if (connection.State != ConnectionState.Open)
                throw new ConnectionClosedException(connection);

            command.Connection = connection;
            command.Transaction = transaction;

            // Get a lazy command instance
            var lazyCommand = this.commands.GetOrAdd(commandKey, k => new Lazy<SyncPreparedCommand>(() =>
            {
                var syncCommand = new SyncPreparedCommand(commandKey);
                return syncCommand;
            }));

            // lazyCommand.Metadata is a boolean indicating if the command is already prepared on the server
            if (lazyCommand.Value.IsPrepared == true)
                return command;

            // Testing The Prepare() performance increase
            command.Prepare();

            // Adding this command as prepared
            lazyCommand.Value.IsPrepared = true;

            this.commands.AddOrUpdate(commandKey, lazyCommand, (key, lc) => new Lazy<SyncPreparedCommand>(() => lc.Value));

            return command;
        }
    }
}