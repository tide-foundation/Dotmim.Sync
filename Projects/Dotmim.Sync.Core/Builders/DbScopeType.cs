namespace Dotmim.Sync.Builders
{

    /// <summary>
    /// Scope type enumeration.
    /// </summary>
    public enum DbScopeType
    {
        /// <summary>
        /// Scope info table.
        /// </summary>
        ScopeInfo,

        /// <summary>
        /// Scope info client table.
        /// </summary>
        ScopeInfoClient,

        /// <summary>
        /// Tide fork (atomic-errors-batch): per-row, per-scope errors-batch table
        /// kept in the client database so that recording a failed-to-apply row and
        /// applying the surrounding data rows happen inside one DB transaction.
        /// Replaces the file-based <c>sync_scope_errors</c> BatchInfo blob.
        /// </summary>
        ScopeInfoClientErrors,
    }
}