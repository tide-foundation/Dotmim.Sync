// Tide fork (atomic-errors-batch): per-row persistence for failed-to-apply
// data rows, replacing the legacy on-disk BatchInfo blob pointed at by
// scope_info_client.sync_scope_errors.
//
// The legacy upstream flow records failures by writing JSON files OUTSIDE
// the data-apply transaction and only then committing the transaction. A
// kill between commit and flush (or mid-flush) advances the sync watermark
// while losing the record of dropped rows — i.e. silent data loss.
//
// This file adds a transaction-aware path: rows go through INSERT OR
// REPLACE into a SQLite table living in the same database file as the
// data tables, using the same DbConnection and DbTransaction as the
// apply. Either both commit or both roll back.
//
// Wire format for row payloads:
//   primary_key_json := JSON array of the row's PK column values (the
//                       same JSON encoding LocalJsonSerializer uses,
//                       one cell per PK column).
//   row_payload_json := JSON array of every cell in SyncRow.ToArray(),
//                       including the leading row-state slot. This is the
//                       exact shape WriteRowToFileAsync writes per row,
//                       so re-materialising into a SyncRow via the same
//                       SyncRow(SchemaTable, object[]) constructor works
//                       without further translation.
//
// PROVIDER FALLBACK
// Providers that haven't overridden the new SaveScopeInfoClientErrorRow
// command (Postgres, SqlServer, etc.) keep the legacy file-based path —
// the helpers below return null command and the orchestrator falls
// through to the file writer. Only SQLite (the client store) is migrated.

using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Extensions;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Tide fork (atomic-errors-batch): per-row errors-batch persistence layer.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Tide fork: tests whether the active provider supports the
        /// per-row, in-DB errors-batch table. If false, the caller falls
        /// back to the legacy file-based BatchInfo path.
        /// </summary>
        internal bool InternalSupportsErrorsBatchTable(DbConnection connection, DbTransaction transaction)
        {
            if (this.Provider == null)
                return false;

            try
            {
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                if (scopeBuilder == null)
                    return false;

                using var probe = scopeBuilder.GetCommandAsync(
                    DbScopeCommandType.SaveScopeInfoClientErrorRow, connection, transaction);

                return probe != null;
            }
            catch
            {
                // Defensive — never let the feature-probe fail a sync.
                return false;
            }
        }

        /// <summary>
        /// Tide fork: loads every stored failed-row record for a single
        /// scope, grouped by (table_name, table_schema) and rehydrated
        /// into SyncRow instances against the provided schema tables.
        ///
        /// Returns an empty dictionary if the provider does not support
        /// the in-DB errors-batch table or no rows are present.
        /// </summary>
        internal async Task<Dictionary<(string TableName, string SchemaName), List<SyncRow>>>
            InternalLoadScopeErrorRowsAsync(
                ScopeInfo scopeInfo,
                string scopeName,
                SyncContext context,
                DbConnection connection,
                DbTransaction transaction,
                IProgress<ProgressArgs> progress,
                CancellationToken cancellationToken)
        {
            var result = new Dictionary<(string, string), List<SyncRow>>();

            if (!this.InternalSupportsErrorsBatchTable(connection, transaction))
                return result;

            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);
            using var loadCommand = scopeBuilder.GetCommandAsync(
                DbScopeCommandType.LoadScopeInfoClientErrorRows, connection, transaction);

            if (loadCommand == null)
                return result;

            SetParameterValue(loadCommand, "@sync_scope_name", scopeName ?? string.Empty);

            await this.InterceptAsync(
                new ExecuteCommandArgs(context, loadCommand, default, connection, transaction),
                progress, cancellationToken).ConfigureAwait(false);

            // Build a quick schema-table lookup so we can re-create SyncRows
            // with the correct SchemaTable reference per (table, schema).
            var schemaIndex = new Dictionary<(string, string), SyncTable>();

            foreach (var t in scopeInfo.Schema.Tables)
                schemaIndex[(t.TableName, t.SchemaName ?? string.Empty)] = t;

            using var reader = await loadCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                // Column order matches the SELECT in
                // SqliteScopeBuilder.GetLoadScopeInfoClientErrorRowsCommand:
                //   0: sync_scope_name
                //   1: table_name
                //   2: table_schema
                //   3: row_state
                //   4: primary_key_json (unused on load — kept as identity)
                //   5: row_payload_json
                //   6: created_at_ticks (unused on load)
                var tableName = reader.GetString(1);
                var tableSchema = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
                var rowState = (SyncRowState)reader.GetInt32(3);
                var rowPayloadJson = reader.GetString(5);

                if (!schemaIndex.TryGetValue((tableName, tableSchema), out var schemaTable))
                {
                    this.Logger.LogWarning(
                        "[InternalLoadScopeErrorRowsAsync] No schema for ({TableName}, {SchemaName}) in scope {Scope}; dropping stored failed row.",
                        tableName, tableSchema, scopeName);
                    continue;
                }

                SyncRow syncRow;

                try
                {
                    syncRow = DeserializeSyncRow(rowPayloadJson, schemaTable, rowState);
                }
                catch (Exception ex) when (
                    ex is JsonException ||
                    ex is ArgumentOutOfRangeException ||
                    ex is InvalidOperationException)
                {
                    // Defensive — see Bug 2 reasoning in InternalApplyCleanErrorsAsync.
                    // A row that fails to parse here would have been a corrupt JSON
                    // file in the legacy world. Log + drop.
                    this.Logger.LogWarning(
                        ex,
                        "[InternalLoadScopeErrorRowsAsync] Corrupt row_payload_json for ({TableName}, {SchemaName}); dropping.",
                        tableName, tableSchema);
                    continue;
                }

                if (!result.TryGetValue((tableName, tableSchema), out var list))
                {
                    list = new List<SyncRow>();
                    result[(tableName, tableSchema)] = list;
                }

                list.Add(syncRow);
            }

            return result;
        }

        /// <summary>
        /// Tide fork: materialises the per-row errors-batch from SQLite into
        /// an on-disk BatchInfo so the existing
        /// <c>InternalApplyCleanErrorsAsync</c> / <c>InternalApplyChangesAsync</c>
        /// paths can read it via <c>LocalJsonSerializer</c> unchanged.
        ///
        /// The materialised files live under <c>Options.BatchDirectory</c>
        /// with a fresh per-sync directory and are write-only here — the
        /// reader path consumes them and the writer path at end-of-sync
        /// will replace the SQLite contents directly (these files are not
        /// re-used across syncs).
        ///
        /// Returns null if there are no rows to materialise or the provider
        /// doesn't support the in-DB table.
        /// </summary>
        internal async Task<BatchInfo> InternalMaterializeScopeErrorsBatchInfoAsync(
            ScopeInfo scopeInfo,
            string scopeName,
            SyncContext context,
            DbConnection connection,
            DbTransaction transaction,
            IProgress<ProgressArgs> progress,
            CancellationToken cancellationToken)
        {
            var byTable = await this.InternalLoadScopeErrorRowsAsync(
                scopeInfo, scopeName, context, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

            if (byTable.Count == 0)
                return null;

            // Match the upstream "<db>_ERRORS" info string so existing path
            // conventions and log messages still line up.
            var info = connection != null && !string.IsNullOrEmpty(connection.Database)
                ? $"{connection.Database}_ERRORS"
                : "ERRORS";

            var errorsBatchInfo = new BatchInfo(this.Options.BatchDirectory, info: info);

            var batchIndex = 0;

            foreach (var kv in byTable)
            {
                var (tableName, tableSchema) = kv.Key;
                var rows = kv.Value;

                if (rows.Count == 0)
                    continue;

                // Find the matching schema table in scopeInfo.Schema so the
                // file header matches what the reader expects.
                var schemaTable = scopeInfo.Schema.Tables.FirstOrDefault(t =>
                    t.TableName.Equals(tableName, SyncGlobalization.DataSourceStringComparison) &&
                    (t.SchemaName ?? string.Empty).Equals(tableSchema, SyncGlobalization.DataSourceStringComparison));

                if (schemaTable == null)
                    continue;

                // Build a temporary SyncTable for the batch part — same
                // construction LocalOrchestrator does for the legacy writer.
                var changesSet = schemaTable.Schema.Clone(false);
                var changesTable = CreateChangesTable(schemaTable, changesSet);

                using var localSerializer = new LocalJsonSerializer(this, context);

                var (filePath, fileName) = errorsBatchInfo.GetNewBatchPartInfoPath(
                    changesTable, batchIndex, "json", info);

                var batchPartInfo = new BatchPartInfo(
                    fileName, changesTable.TableName, changesTable.SchemaName, SyncRowState.None, rows.Count, batchIndex);

                errorsBatchInfo.BatchPartsInfo.Add(batchPartInfo);

                await localSerializer.OpenFileAsync(filePath, changesTable, SyncRowState.None).ConfigureAwait(false);

                foreach (var row in rows)
                {
                    // Rebind the row's SchemaTable to changesTable (which is what
                    // LocalJsonSerializer.WriteRowToFileAsync uses to compute the
                    // column count and header). Since changesTable.Columns is a
                    // clone of schemaTable.Columns with the same ordering, the
                    // buffer object[] from row.ToArray() lines up.
                    row.SchemaTable = changesTable;
                    await localSerializer.WriteRowToFileAsync(row, changesTable).ConfigureAwait(false);
                }

                batchIndex++;
            }

            return errorsBatchInfo.BatchPartsInfo.Count > 0 ? errorsBatchInfo : null;
        }

        /// <summary>
        /// Tide fork: replaces the entire per-row errors-batch for a scope
        /// in a single transaction with the rows present in the supplied
        /// <see cref="SyncSet"/>. Returns the number of rows persisted.
        ///
        /// Behaviour:
        ///   1. DELETE FROM scope_info_client_errors WHERE sync_scope_name = @scope
        ///   2. INSERT OR REPLACE for each row in failedRows.Tables
        ///
        /// All operations use the supplied <paramref name="connection"/> and
        /// <paramref name="transaction"/>, which are the same ones the data
        /// apply was using. That is the whole point: the writer and the data
        /// apply commit atomically.
        ///
        /// Returns -1 if the provider doesn't support the in-DB errors-batch.
        /// </summary>
        internal async Task<int> InternalReplaceScopeErrorRowsAsync(
            string scopeName,
            SyncContext context,
            SyncSet failedRows,
            DbConnection connection,
            DbTransaction transaction,
            IProgress<ProgressArgs> progress,
            CancellationToken cancellationToken)
        {
            if (!this.InternalSupportsErrorsBatchTable(connection, transaction))
                return -1;

            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            // Step 1: wipe any prior failed rows for this scope.
            using (var deleteAllCommand = scopeBuilder.GetCommandAsync(
                       DbScopeCommandType.DeleteScopeInfoClientErrorRowsForScope, connection, transaction))
            {
                if (deleteAllCommand == null)
                    return -1;

                SetParameterValue(deleteAllCommand, "@sync_scope_name", scopeName ?? string.Empty);

                await this.InterceptAsync(
                    new ExecuteCommandArgs(context, deleteAllCommand, default, connection, transaction),
                    progress, cancellationToken).ConfigureAwait(false);

                await deleteAllCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            // Step 2: insert the new failed rows (if any).
            var inserted = 0;

            if (failedRows == null)
                return inserted;

            // We pull the SAVE command once per scope and reuse it across rows
            // by re-setting parameters — much cheaper than recreating the
            // command for every row when there are many failures.
            using var saveCommand = scopeBuilder.GetCommandAsync(
                DbScopeCommandType.SaveScopeInfoClientErrorRow, connection, transaction);

            if (saveCommand == null)
                return -1;

            var nowTicks = DateTime.UtcNow.Ticks;

            foreach (var table in failedRows.Tables)
            {
                if (!table.HasRows)
                    continue;

                foreach (var row in table.Rows)
                {
                    var pkJson = SerializePrimaryKey(row, table);
                    var payloadJson = SerializeRowPayload(row);

                    SetParameterValue(saveCommand, "@sync_scope_name", scopeName ?? string.Empty);
                    SetParameterValue(saveCommand, "@table_name", table.TableName);
                    SetParameterValue(saveCommand, "@table_schema", table.SchemaName ?? string.Empty);
                    SetParameterValue(saveCommand, "@row_state", (int)row.RowState);
                    SetParameterValue(saveCommand, "@primary_key_json", pkJson);
                    SetParameterValue(saveCommand, "@row_payload_json", payloadJson);
                    SetParameterValue(saveCommand, "@created_at_ticks", nowTicks);

                    await this.InterceptAsync(
                        new ExecuteCommandArgs(context, saveCommand, default, connection, transaction),
                        progress, cancellationToken).ConfigureAwait(false);

                    await saveCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    inserted++;
                }
            }

            return inserted;
        }

        /// <summary>
        /// Tide fork: legacy migration. Called once during provisioning,
        /// after the new <c>scope_info_client_errors</c> table has been
        /// created. NULLs out every <c>sync_scope_errors</c> blob in
        /// <c>scope_info_client</c> so the upstream
        /// <c>InternalApplyCleanErrorsAsync</c> path stops touching dead
        /// JSON files. Idempotent: subsequent provisions are no-ops.
        ///
        /// Any orphaned JSON files under <c>Options.BatchDirectory</c> from
        /// pre-upgrade syncs are left alone — they'll be cleaned by the
        /// container restart / <c>/tmp</c> sweep. They cannot be reached
        /// by the new code path because the pointer column is now NULL.
        /// </summary>
        internal async Task InternalMigrateLegacyScopeErrorsAsync(
            SyncContext context,
            DbConnection connection,
            DbTransaction transaction,
            IProgress<ProgressArgs> progress,
            CancellationToken cancellationToken)
        {
            // Resolve the scope_info_client table name from the scope
            // builder so the UPDATE matches whatever prefix the host app
            // uses (e.g. tiden in ORK).
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            if (scopeBuilder == null)
                return;

            var scopeNames = scopeBuilder.GetParsedScopeInfoTableNames();
            var scopeInfoClientTableName = $"{scopeNames.NormalizedName}_client";

            using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText =
                $"UPDATE [{scopeInfoClientTableName}] SET sync_scope_errors = NULL WHERE sync_scope_errors IS NOT NULL";

            try
            {
                await this.InterceptAsync(
                    new ExecuteCommandArgs(context, cmd, default, connection, transaction),
                    progress, cancellationToken).ConfigureAwait(false);

                var affected = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                if (affected > 0)
                {
                    this.Logger.LogInformation(
                        "[InternalMigrateLegacyScopeErrorsAsync] Cleared {Count} legacy sync_scope_errors blob(s) from {Table}.",
                        affected, scopeInfoClientTableName);
                }
            }
            catch (DbException dbEx)
            {
                // If scope_info_client doesn't exist yet, or sync_scope_errors
                // is somehow missing, log and move on — the new path doesn't
                // need this UPDATE to succeed to function correctly.
                this.Logger.LogDebug(
                    dbEx,
                    "[InternalMigrateLegacyScopeErrorsAsync] Non-fatal during legacy clear on {Table}: {Message}",
                    scopeInfoClientTableName, dbEx.Message);
            }
        }

        // ----- helpers -----

        /// <summary>
        /// Tide fork: extracts the row's primary-key cell values and encodes
        /// them as a JSON array. The same Serializer used by
        /// <see cref="LocalJsonSerializer"/> is used so encoding is uniform.
        /// </summary>
        private static string SerializePrimaryKey(SyncRow row, SyncTable schemaTable)
        {
            var pkColumns = schemaTable.GetPrimaryKeysColumns().ToList();
            var sb = new StringBuilder();
            sb.Append('[');

            for (var i = 0; i < pkColumns.Count; i++)
            {
                if (i > 0)
                    sb.Append(',');

                var value = row[pkColumns[i].ColumnName];
                sb.Append(Serializer.Serialize(value).ToUtf8String());
            }

            sb.Append(']');
            return sb.ToString();
        }

        /// <summary>
        /// Tide fork: encodes the full row buffer (including the leading
        /// row-state slot) as a JSON array — the exact shape
        /// <see cref="LocalJsonSerializer.WriteRowToFileAsync"/> writes.
        /// </summary>
        private static string SerializeRowPayload(SyncRow row)
        {
            var inner = row.ToArray();
            var sb = new StringBuilder();
            sb.Append('[');

            for (var i = 0; i < inner.Length; i++)
            {
                if (i > 0)
                    sb.Append(',');

                sb.Append(Serializer.Serialize(inner[i]).ToUtf8String());
            }

            sb.Append(']');
            return sb.ToString();
        }

        /// <summary>
        /// Tide fork: decodes a row_payload_json blob back into a SyncRow
        /// using the supplied schema table. The blob is the JSON array
        /// produced by <see cref="SerializeRowPayload(SyncRow)"/>; element 0
        /// is the row-state slot, elements 1..N are column values.
        /// </summary>
        private static SyncRow DeserializeSyncRow(string rowPayloadJson, SyncTable schemaTable, SyncRowState fallbackState)
        {
            // The +1 buffer slot at index 0 is the row state, mirroring
            // SyncRow.ctor(SyncTable, object[]).
            var expectedLen = schemaTable.Columns.Count + 1;
            var buffer = new object[expectedLen];

            using var doc = JsonDocument.Parse(rowPayloadJson);

            if (doc.RootElement.ValueKind != JsonValueKind.Array)
                throw new JsonException("row_payload_json root must be a JSON array");

            var arr = doc.RootElement;
            var arrLen = arr.GetArrayLength();

            // Be defensive: if the stored payload was shorter than the
            // schema (e.g. a schema migration added a column), pad with
            // null on the right; if it's longer, truncate. The composite
            // PK still locates the row uniquely.
            for (var i = 0; i < expectedLen; i++)
            {
                if (i < arrLen)
                {
                    var el = arr[i];
                    buffer[i] = JsonElementToObject(el);
                }
                else
                {
                    buffer[i] = null;
                }
            }

            // Make sure slot 0 (row state) is populated. If the payload was
            // produced before this fork the column count expectation will
            // still be schema.Columns.Count + 1 — slot 0 will be set.
            if (buffer[0] == null)
                buffer[0] = (int)fallbackState;

            return new SyncRow(schemaTable, buffer);
        }

        /// <summary>
        /// Tide fork: minimal JsonElement->object conversion that mirrors
        /// the inverse of <see cref="Serializer.Serialize(object)"/>. The
        /// downstream SyncRow consumers coerce via <c>SyncTypeConverter</c>,
        /// so returning broad CLR types (string/long/double/bool/null) is
        /// sufficient.
        /// </summary>
        private static object JsonElementToObject(JsonElement el) => el.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.Undefined => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? l : (object)el.GetDouble(),
            // Object / Array: keep the raw JSON; SyncTypeConverter will
            // coerce on-demand at the consumer site if needed.
            _ => el.GetRawText(),
        };

        /// <summary>
        /// Tide fork: looks up a DbCommand parameter by name and assigns
        /// its value, normalising null -> DBNull.Value. Required because
        /// the per-row save command is reused across many rows.
        /// </summary>
        private static void SetParameterValue(DbCommand command, string parameterName, object value)
        {
            if (!command.Parameters.Contains(parameterName))
                return;

            var p = command.Parameters[parameterName];
            p.Value = value ?? DBNull.Value;
        }
    }
}
