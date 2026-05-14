using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Microsoft.Data.Sqlite;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Sqlite
{
    /// <summary>
    /// Sqlite scope builder.
    /// </summary>
    public class SqliteScopeBuilder : DbScopeBuilder
    {
        /// <summary>
        /// Gets the scope info table names.
        /// </summary>
        protected DbTableNames ScopeInfoTableNames { get; }

        /// <summary>
        /// Gets the scope info client table names.
        /// </summary>
        protected DbTableNames ScopeInfoClientTableNames { get; }

        /// <inheritdoc cref="SqliteScopeBuilder" />
        public SqliteScopeBuilder(string scopeInfoTableName)
        {
            var tableParser = new TableParser(scopeInfoTableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

            this.ScopeInfoTableNames = new DbTableNames(SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote,
                tableParser.TableName, tableParser.NormalizedFullName, tableParser.NormalizedShortName,
                tableParser.QuotedFullName, tableParser.QuotedShortName, tableParser.SchemaName);

            var scopeInfoClientFullTableName = $"[{tableParser.TableName}_client]";

            tableParser = new TableParser(scopeInfoClientFullTableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

            this.ScopeInfoClientTableNames = new DbTableNames(SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote,
                tableParser.TableName, tableParser.NormalizedFullName, tableParser.NormalizedShortName,
                tableParser.QuotedFullName, tableParser.QuotedShortName, tableParser.SchemaName);
        }

        /// <inheritdoc />
        public override DbTableNames GetParsedScopeInfoTableNames() => this.ScopeInfoTableNames;

        /// <inheritdoc />
        public override DbTableNames GetParsedScopeInfoClientTableNames() => this.ScopeInfoClientTableNames;

        /// <inheritdoc />
        public override DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"Select {SqliteObjectNames.TimestampValue}";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetExistsScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{this.ScopeInfoTableNames.NormalizedName}'";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetExistsScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{this.ScopeInfoTableNames.NormalizedName}_client'";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetExistsScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"Select count(*) from {this.ScopeInfoTableNames.NormalizedName} where sync_scope_name = @sync_scope_name";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p0 = command.CreateParameter();
            p0.ParameterName = "@sync_scope_name";
            p0.DbType = DbType.String;
            p0.Size = 100;
            command.Parameters.Add(p0);
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetExistsScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.CommandText = $@"Select count(*) from [{this.ScopeInfoTableNames.NormalizedName}_client] 
                                     where sync_scope_id = @sync_scope_id 
                                     and sync_scope_name = @sync_scope_name
                                     and sync_scope_hash = @sync_scope_hash;";

            command.Transaction = transaction;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetCreateScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                    $@"CREATE TABLE [{this.ScopeInfoTableNames.NormalizedName}](
                        sync_scope_name text NOT NULL,
                        sync_scope_schema text NULL,
                        sync_scope_setup text NULL,
                        sync_scope_version text NULL,
                        sync_scope_last_clean_timestamp integer NULL,
                        sync_scope_properties text NULL,
                        CONSTRAINT PKey_{this.ScopeInfoTableNames.NormalizedName} PRIMARY KEY(sync_scope_name))";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetCreateScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                    $@"CREATE TABLE [{this.ScopeInfoTableNames.NormalizedName}_client](
                        sync_scope_id blob NOT NULL,
                        sync_scope_name text NOT NULL,
                        sync_scope_hash text NOT NULL,
                        sync_scope_parameters text NULL,
                        scope_last_sync_timestamp integer NULL,
                        scope_last_server_sync_timestamp integer NULL,
                        scope_last_sync_duration integer NULL,
                        scope_last_sync datetime NULL,
                        sync_scope_errors text NULL,
                        sync_scope_properties text NULL,
                        CONSTRAINT PKey_{this.ScopeInfoTableNames.NormalizedName}_client PRIMARY KEY(sync_scope_id, sync_scope_name, sync_scope_hash))";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetAllScopeInfosCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                $@"SELECT [sync_scope_name], 
                          [sync_scope_schema], 
                          [sync_scope_setup], 
                          [sync_scope_version],
                          [sync_scope_last_clean_timestamp],
                          [sync_scope_properties]
                    FROM  {this.ScopeInfoTableNames.NormalizedName}";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetAllScopeInfoClientsCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                $@"SELECT  [sync_scope_id]
                         , [sync_scope_name]
                         , [sync_scope_hash]
                         , [sync_scope_parameters]
                         , [scope_last_sync_timestamp]
                         , [scope_last_server_sync_timestamp]
                         , [scope_last_sync_duration]
                         , [scope_last_sync]
                         , [sync_scope_errors]
                         , [sync_scope_properties]
                    FROM  [{this.ScopeInfoTableNames.NormalizedName}_client]";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetDeleteScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"DELETE FROM [{this.ScopeInfoTableNames.NormalizedName}] WHERE [sync_scope_name] = @sync_scope_name";

            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetDeleteScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                $@"DELETE FROM [{this.ScopeInfoTableNames.NormalizedName}_client]
                   WHERE [sync_scope_name] = @sync_scope_name and [sync_scope_id] = @sync_scope_id and [sync_scope_hash] = @sync_scope_hash";

            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableNames.NormalizedName;

            var commandText =
                    $@"SELECT [sync_scope_name], 
                          [sync_scope_schema], 
                          [sync_scope_setup], 
                          [sync_scope_version],
                          [sync_scope_last_clean_timestamp],
                          [sync_scope_properties]
                    FROM  [{tableName}]
                    WHERE [sync_scope_name] = @sync_scope_name";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableNames.NormalizedName}_client";
            var commandText =
                $@"SELECT    [sync_scope_id]
                           , [sync_scope_name]
                           , [sync_scope_hash]
                           , [sync_scope_parameters]
                           , [scope_last_sync_timestamp]
                           , [scope_last_server_sync_timestamp]
                           , [scope_last_sync_duration]
                           , [scope_last_sync]
                           , [sync_scope_errors]
                           , [sync_scope_properties]
                    FROM  [{tableName}]
                    WHERE [sync_scope_name] = @sync_scope_name and [sync_scope_id] = @sync_scope_id and [sync_scope_hash] = @sync_scope_hash";

            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            var p0 = command.CreateParameter();
            p0.ParameterName = "@sync_scope_id";
            p0.DbType = DbType.Guid;
            command.Parameters.Add(p0);

            var p1 = command.CreateParameter();
            p1.ParameterName = "@sync_scope_hash";
            p1.DbType = DbType.String;
            p1.Size = 100;
            command.Parameters.Add(p1);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetInsertScopeInfoCommand(DbConnection connection, DbTransaction transaction)
            => this.GetSaveScopeInfoCommand(false, connection, transaction);

        /// <inheritdoc />
        public override DbCommand GetInsertScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
            => this.GetSaveScopeInfoClientCommand(false, connection, transaction);

        /// <inheritdoc />
        public override DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction)
            => this.GetSaveScopeInfoCommand(true, connection, transaction);

        /// <inheritdoc />
        public override DbCommand GetUpdateScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
            => this.GetSaveScopeInfoClientCommand(true, connection, transaction);

        /// <summary>
        /// Gets the save scope info command.
        /// </summary>
        public DbCommand GetSaveScopeInfoCommand(bool exist, DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableNames.NormalizedName;

            var stmtText = new StringBuilder();

            stmtText.AppendLine(exist
                    ? $"UPDATE {tableName} " +
                      $"SET sync_scope_schema=@sync_scope_schema, " +
                      $"sync_scope_setup=@sync_scope_setup, " +
                      $"sync_scope_version=@sync_scope_version, " +
                      $"sync_scope_last_clean_timestamp=@sync_scope_last_clean_timestamp, " +
                      $"sync_scope_properties=@sync_scope_properties " +
                      $"WHERE sync_scope_name=@sync_scope_name;"

                    : $"INSERT INTO {tableName} " +
                      $"(sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, " +
                      $"sync_scope_last_clean_timestamp, sync_scope_properties) " +
                      $"VALUES " +
                      $"(@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, " +
                      $"@sync_scope_last_clean_timestamp, @sync_scope_properties);");

            stmtText.AppendLine(@$"SELECT sync_scope_name
                           , sync_scope_schema
                           , sync_scope_setup
                           , sync_scope_version
                           , sync_scope_last_clean_timestamp
                           , sync_scope_properties
                    FROM  {tableName}
                    WHERE sync_scope_name=@sync_scope_name;");

            var command = new SqliteCommand(stmtText.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction);

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_schema";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_setup";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_version";
            p.DbType = DbType.String;
            p.Size = 10;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_last_clean_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_properties";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;
        }

        /// <summary>
        /// Gets the save scope info client command.
        /// </summary>
        public DbCommand GetSaveScopeInfoClientCommand(bool exist, DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableNames.NormalizedName}_client";

            var stmtText = new StringBuilder();

            stmtText.AppendLine(exist
                    ? $"UPDATE {tableName} " +
                      $"SET scope_last_sync_timestamp=@scope_last_sync_timestamp, " +
                      $"scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp, " +
                      $"scope_last_sync=@scope_last_sync, " +
                      $"scope_last_sync_duration=@scope_last_sync_duration, " +
                      $"sync_scope_properties=@sync_scope_properties,  " +
                      $"sync_scope_errors=@sync_scope_errors,  " +
                      $"sync_scope_parameters=@sync_scope_parameters  " +
                      $"WHERE sync_scope_id=@sync_scope_id and sync_scope_name=@sync_scope_name and sync_scope_hash=@sync_scope_hash;"

                    : $"INSERT INTO {tableName} " +
                      $"(sync_scope_name, sync_scope_id, sync_scope_hash, sync_scope_parameters, scope_last_sync_timestamp, scope_last_server_sync_timestamp, " +
                      $"scope_last_sync, scope_last_sync_duration, sync_scope_errors, sync_scope_properties) " +
                      $"VALUES " +
                      $"(@sync_scope_name, @sync_scope_id, @sync_scope_hash, @sync_scope_parameters, @scope_last_sync_timestamp, @scope_last_server_sync_timestamp, " +
                      $"@scope_last_sync, @scope_last_sync_duration, @sync_scope_errors, @sync_scope_properties);");

            stmtText.AppendLine(@$"SELECT sync_scope_id
                           , sync_scope_name
                           , sync_scope_hash
                           , sync_scope_parameters
                           , scope_last_sync_timestamp
                           , scope_last_server_sync_timestamp
                           , scope_last_sync
                           , scope_last_sync_duration
                           , sync_scope_errors
                           , sync_scope_properties
                    FROM  {tableName}
                    WHERE sync_scope_name=@sync_scope_name and sync_scope_id=@sync_scope_id and sync_scope_hash=@sync_scope_hash;");

            var command = new SqliteCommand(stmtText.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction);

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_parameters";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_server_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync";
            p.DbType = DbType.DateTime;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_duration";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_errors";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_properties";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetDropScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableNames.NormalizedName;

            var commandText = $"DROP TABLE {tableName}";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetDropScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {

            var commandText = $"DROP TABLE {this.ScopeInfoTableNames.NormalizedName}_client";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }

        // =================================================================
        // Tide fork (atomic-errors-batch): per-row errors-batch table.
        //
        // The legacy upstream behaviour is to:
        //   (a) apply incoming rows to the data tables in one transaction,
        //   (b) write any failed rows to a JSON file under
        //       Options.BatchDirectory via LocalJsonSerializer, OUTSIDE the
        //       transaction,
        //   (c) commit the transaction (advancing the watermark).
        //
        // If the process is killed between (a) and (b), or in the middle of
        // (b), the watermark may advance with no record of the failed rows.
        // We have observed this on a deployed ORK node — three users (UIDs
        // 517-519) silently dropped, with master->ork tracking timestamps
        // strictly less than the client's scope_last_server_sync_timestamp.
        //
        // The fix is to record failed rows in a SQLite table living in the
        // SAME database file as the data tables, using the SAME DbConnection
        // and DbTransaction. Apply + record-failure now commit atomically.
        //
        // Schema:
        //   sync_scope_name      TEXT    NOT NULL   -- scope identity
        //   table_name           TEXT    NOT NULL   -- failed row's table
        //   table_schema         TEXT    NOT NULL DEFAULT ''  -- table's schema (often "")
        //   row_state            INTEGER NOT NULL   -- SyncRowState enum value
        //   primary_key_json     TEXT    NOT NULL   -- JSON-serialized PK tuple, identity
        //   row_payload_json     TEXT    NOT NULL   -- JSON-serialized row values, "[v0, v1, ...]"
        //   created_at_ticks     INTEGER NOT NULL   -- DateTime.UtcNow.Ticks at first insert
        //   PRIMARY KEY (sync_scope_name, table_name, table_schema, primary_key_json)
        //
        // row_payload_json carries the full row as the SyncRow.ToArray()
        // shape, encoded as a JSON array (the same shape LocalJsonSerializer
        // writes per row). This lets the orchestrator's clean-errors path
        // re-hydrate rows via the standard column-coercion logic.
        //
        // The expected workload is small (10s of rows per scope under
        // normal operation, with a bounded retain window). A single scope
        // index suffices.
        // =================================================================

        /// <summary>
        /// Tide fork: errors-batch table name, sibling of scope_info_client.
        /// </summary>
        private string ScopeInfoClientErrorsTableName => $"{this.ScopeInfoTableNames.NormalizedName}_client_errors";

        /// <inheritdoc />
        public override DbCommand GetExistsScopeInfoClientErrorsTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{this.ScopeInfoClientErrorsTableName}'";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetCreateScopeInfoClientErrorsTableCommand(DbConnection connection, DbTransaction transaction)
        {
            // CREATE TABLE only — no trailing CREATE INDEX. The caller
            // (DbScopeBuilder.GetCommandAsync) invokes SqliteCommand.Prepare()
            // before ExecuteNonQuery; SQLite's prepare compiles every statement
            // in the script upfront, so a "CREATE TABLE T; CREATE INDEX ... ON T"
            // pair fails with "no such table: T" because the second statement is
            // compiled before the first executes.
            //
            // The primary key (sync_scope_name, table_name, table_schema,
            // primary_key_json) already builds a leftmost index on
            // sync_scope_name, which is the only column the runtime LOAD query
            // filters on — so the previously-included redundant
            // ix_{tableName}_scope index added no measurable benefit. With low
            // row counts (errors are by definition rare) the PK index is
            // sufficient.
            var tableName = this.ScopeInfoClientErrorsTableName;
            var commandText =
                $@"CREATE TABLE [{tableName}](
                    sync_scope_name    TEXT    NOT NULL,
                    table_name         TEXT    NOT NULL,
                    table_schema       TEXT    NOT NULL DEFAULT '',
                    row_state          INTEGER NOT NULL,
                    primary_key_json   TEXT    NOT NULL,
                    row_payload_json   TEXT    NOT NULL,
                    created_at_ticks   INTEGER NOT NULL,
                    CONSTRAINT PKey_{tableName} PRIMARY KEY(sync_scope_name, table_name, table_schema, primary_key_json));";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetDropScopeInfoClientErrorsTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"DROP TABLE [{this.ScopeInfoClientErrorsTableName}]";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetLoadScopeInfoClientErrorRowsCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoClientErrorsTableName;
            var commandText =
                $@"SELECT sync_scope_name, table_name, table_schema, row_state, primary_key_json, row_payload_json, created_at_ticks
                   FROM [{tableName}]
                   WHERE sync_scope_name = @sync_scope_name
                   ORDER BY table_name, table_schema, primary_key_json";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetSaveScopeInfoClientErrorRowCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoClientErrorsTableName;

            // INSERT OR REPLACE so we upsert by the composite PK. The
            // created_at_ticks is supplied by the caller; on a replace we
            // keep the new timestamp (good enough for cleanup heuristics).
            var commandText =
                $@"INSERT OR REPLACE INTO [{tableName}]
                       (sync_scope_name, table_name, table_schema, row_state, primary_key_json, row_payload_json, created_at_ticks)
                   VALUES
                       (@sync_scope_name, @table_name, @table_schema, @row_state, @primary_key_json, @row_payload_json, @created_at_ticks)";

            var command = new SqliteCommand(commandText, (SqliteConnection)connection, (SqliteTransaction)transaction);

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@table_name";
            p.DbType = DbType.String;
            p.Size = 200;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@table_schema";
            p.DbType = DbType.String;
            p.Size = 200;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@row_state";
            p.DbType = DbType.Int32;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@primary_key_json";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@row_payload_json";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@created_at_ticks";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetDeleteScopeInfoClientErrorRowCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoClientErrorsTableName;
            var commandText =
                $@"DELETE FROM [{tableName}]
                   WHERE sync_scope_name = @sync_scope_name
                     AND table_name      = @table_name
                     AND table_schema    = @table_schema
                     AND primary_key_json = @primary_key_json";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@table_name";
            p.DbType = DbType.String;
            p.Size = 200;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@table_schema";
            p.DbType = DbType.String;
            p.Size = 200;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@primary_key_json";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetDeleteScopeInfoClientErrorRowsForScopeCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoClientErrorsTableName;
            var commandText =
                $@"DELETE FROM [{tableName}] WHERE sync_scope_name = @sync_scope_name";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }
    }
}