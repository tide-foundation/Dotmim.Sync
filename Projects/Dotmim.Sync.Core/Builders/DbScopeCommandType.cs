using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// All commands type for the scope info tables used during sync.
    /// </summary>
    public enum DbScopeCommandType
    {
        /// <summary>
        /// Exists scope info table command.
        /// </summary>
        ExistsScopeInfoTable,

        /// <summary>
        /// Exists scope info client table command.
        /// </summary>
        ExistsScopeInfoClientTable,

        /// <summary>
        /// Create scope info table command.
        /// </summary>
        CreateScopeInfoTable,

        /// <summary>
        /// Create scope info client table command.
        /// </summary>
        CreateScopeInfoClientTable,

        /// <summary>
        /// Drop scope info table command.
        /// </summary>
        DropScopeInfoTable,

        /// <summary>
        /// Drop scope info client table command.
        /// </summary>
        DropScopeInfoClientTable,

        /// <summary>
        /// Get all scope info command.
        /// </summary>
        GetAllScopeInfos,

        /// <summary>
        /// Get all scope info clients command.
        /// </summary>
        GetAllScopeInfoClients,

        /// <summary>
        /// Get scope info command.
        /// </summary>
        GetScopeInfo,

        /// <summary>
        /// Get scope info client command.
        /// </summary>
        GetScopeInfoClient,

        /// <summary>
        /// Insert scope info command.
        /// </summary>
        InsertScopeInfo,

        /// <summary>
        /// Insert scope info client command.
        /// </summary>
        InsertScopeInfoClient,

        /// <summary>
        /// Update scope info command.
        /// </summary>
        UpdateScopeInfo,

        /// <summary>
        /// Update scope info client command.
        /// </summary>
        UpdateScopeInfoClient,

        /// <summary>
        /// Delete scope info command.
        /// </summary>
        DeleteScopeInfo,

        /// <summary>
        /// Delete scope info client command.
        /// </summary>
        DeleteScopeInfoClient,

        /// <summary>
        /// Exist scope info command.
        /// </summary>
        ExistScopeInfo,

        /// <summary>
        /// Exist scope info client command.
        /// </summary>
        ExistScopeInfoClient,

        /// <summary>
        /// Get local timestamp command.
        /// </summary>
        GetLocalTimestamp,

        // -----------------------------------------------------------------
        // Tide fork (atomic-errors-batch): commands for the per-row, per-scope
        // errors-batch table that replaces the file-based sync_scope_errors blob.
        // Implemented by SqliteScopeBuilder; other providers may leave them
        // unimplemented (the orchestrator's fallback path retains the legacy
        // file-based behaviour for those providers).
        // -----------------------------------------------------------------

        /// <summary>
        /// Tide fork: exists scope info client errors table command.
        /// </summary>
        ExistsScopeInfoClientErrorsTable,

        /// <summary>
        /// Tide fork: create scope info client errors table command.
        /// </summary>
        CreateScopeInfoClientErrorsTable,

        /// <summary>
        /// Tide fork: drop scope info client errors table command.
        /// </summary>
        DropScopeInfoClientErrorsTable,

        /// <summary>
        /// Tide fork: load all rows for a given scope (used to materialise the
        /// in-memory failed-rows list at the start of a sync).
        /// </summary>
        LoadScopeInfoClientErrorRows,

        /// <summary>
        /// Tide fork: upsert (delete-by-PK then insert) a single failed row.
        /// </summary>
        SaveScopeInfoClientErrorRow,

        /// <summary>
        /// Tide fork: delete a single failed row by its (scope, table, schema, pk-json) identity.
        /// Used by the clean-errors path after a successful retry.
        /// </summary>
        DeleteScopeInfoClientErrorRow,

        /// <summary>
        /// Tide fork: delete all rows for a given scope (used during the
        /// clean-break migration to discard the legacy file-based errors batch).
        /// </summary>
        DeleteScopeInfoClientErrorRowsForScope,
    }
}