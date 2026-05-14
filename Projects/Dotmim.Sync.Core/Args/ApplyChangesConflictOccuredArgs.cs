using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Raised as an argument when an apply is failing. Waiting from user for the conflict resolution.
    /// </summary>
    public class ApplyChangesConflictOccuredArgs : ProgressArgs
    {
        private readonly ScopeInfo scopeInfo;
        private readonly SyncRow conflictRow;
        private BaseOrchestrator orchestrator;
        private SyncTable schemaChangesTable;
        private SyncConflict conflict;

        /// <inheritdoc cref="ApplyChangesConflictOccuredArgs"/>
        public ApplyChangesConflictOccuredArgs(ScopeInfo scopeInfo, SyncContext context, BaseOrchestrator orchestrator,
            SyncRow conflictRow, SyncTable schemaChangesTable, ConflictResolution action, Guid? senderScopeId, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            Guard.ThrowIfNull(conflictRow);

            this.scopeInfo = scopeInfo;
            this.orchestrator = orchestrator;
            this.conflictRow = conflictRow;
            this.schemaChangesTable = schemaChangesTable;
            this.Resolution = action;
            this.SenderScopeId = senderScopeId;

            // Tide fork: SyncRow(SyncTable, object[]) requires the array
            // length to be exactly schema.Columns.Count + 1 (slot 0 is the
            // row state). If the incoming conflictRow buffer was built with
            // a wider/narrower schema than schemaChangesTable (schema drift
            // between client and server, errors-batch carrying a stale row,
            // etc.) the original code threw ArgumentException("row array has
            // too many items" / "row array must have one more item to store
            // state") from the SyncRow ctor, which propagated as a crit out
            // of the apply pipeline. Re-shape the buffer to the cloned
            // schema instead, so the conflict event still fires and the
            // sync survives. FinalRow is only consumed when the resolution
            // is MergeRow; padding with null / truncating leaves the PK
            // columns intact and any over-/undershoot is logged.
            var clonedSchema = schemaChangesTable.Clone();
            var sourceBuffer = conflictRow.ToArray();
            var expectedLen = clonedSchema.Columns.Count + 1;

            object[] finalRowArray;
            if (sourceBuffer.Length == expectedLen)
            {
                finalRowArray = new object[expectedLen];
                sourceBuffer.CopyTo(finalRowArray, 0);
            }
            else
            {
                orchestrator?.Logger?.LogWarning(
                    "[ApplyChangesConflictOccuredArgs] Skipping row strict-shape check for {Schema}.{Table}: detected schema drift (source buffer has {SrcLen} values but cloned schema expects {ExpectedLen} = Columns.Count + 1). Reshaping conflict row to match cloned schema.",
                    clonedSchema.SchemaName,
                    clonedSchema.TableName,
                    sourceBuffer.Length,
                    expectedLen);

                finalRowArray = new object[expectedLen];
                var copyLen = Math.Min(sourceBuffer.Length, expectedLen);
                Array.Copy(sourceBuffer, finalRowArray, copyLen);

                // Ensure the row-state slot (index 0) is populated even if
                // the source buffer was empty/short.
                if (finalRowArray[0] == null)
                    finalRowArray[0] = (int)conflictRow.RowState;
            }

            try
            {
                this.FinalRow = new SyncRow(clonedSchema, finalRowArray);
            }
            catch (ArgumentException ex)
            {
                // Should be impossible after the reshape above, but if a
                // future change to SyncRow validation re-introduces a throw
                // path we still don't want to take down the sync over a
                // conflict-event side effect.
                orchestrator?.Logger?.LogWarning(
                    ex,
                    "[ApplyChangesConflictOccuredArgs] Skipping row materialization for {Schema}.{Table} due to schema drift; leaving FinalRow null. Conflict resolution will still proceed with the configured policy.",
                    clonedSchema.SchemaName,
                    clonedSchema.TableName);
                this.FinalRow = null;
            }
        }

        /// <summary>
        /// Gets or Sets the action to be taken when resolving the conflict.
        /// If you choose MergeRow, FinalRow will be merged in both sources.
        /// </summary>
        public ConflictResolution Resolution { get; set; }

        /// <summary>
        /// Gets the Progress level used to determine if message is output.
        /// </summary>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets or Sets the scope id who will be marked as winner.
        /// </summary>
        public Guid? SenderScopeId { get; set; }

        /// <summary>
        /// Gets or sets if we have a merge action, the final row represents the merged row.
        /// </summary>
        public SyncRow FinalRow { get; set; }

        /// <summary>
        /// Get the conflict that occurs by selecting the local conflict row.
        /// </summary>
        public async Task<SyncConflict> GetSyncConflictAsync()
        {
            var (_, localRow) = await this.orchestrator.InternalGetConflictRowAsync(this.scopeInfo, this.Context, this.schemaChangesTable, this.conflictRow,
                this.Connection, this.Transaction, default, default).ConfigureAwait(false);
            this.conflict = this.orchestrator.InternalGetConflict(this.Context, this.conflictRow, localRow);
            return this.conflict;
        }

        /// <inheritdoc />
        public override string Message => $"Conflict {this.conflictRow}.";

        /// <inheritdoc />
        public override int EventId => 300;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when a conflict is happening.
        /// </summary>
        public static Guid OnApplyChangesConflictOccured(this BaseOrchestrator orchestrator, Action<ApplyChangesConflictOccuredArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a conflict is happening.
        /// </summary>
        public static Guid OnApplyChangesConflictOccured(this BaseOrchestrator orchestrator, Func<ApplyChangesConflictOccuredArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}