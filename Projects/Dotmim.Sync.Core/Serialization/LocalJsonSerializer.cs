using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Serialization
{
    /// <summary>
    /// Serialize json rows locally.
    /// </summary>
    public class LocalJsonSerializer : IDisposable, IAsyncDisposable
    {
        private static readonly ISerializer Serializer = SerializersFactory.JsonSerializerFactory.GetSerializer();
        private readonly SemaphoreSlim writerLock = new(1, 1);
        private StreamWriter sw;
        private Utf8JsonWriter writer;
        private Func<SyncTable, object[], Task<string>> writingRowAsync;
        private Func<SyncTable, string, Task<object[]>> readingRowAsync;
        private int isOpen;
        private bool disposedValue;

        // Tide fork (errors-batch-resilience): write-to-temp + atomic rename pattern.
        // When append == false we write to <targetPath>.<rand>.tide-tmp and rename on close,
        // so a SIGKILL between Open and Close cannot leave the target truncated mid-row.
        // For append == true these fields stay null and behaviour is unchanged.
        private string targetPath;
        private string tempPath;

        /// <summary>
        /// Initializes a new instance of the <see cref="LocalJsonSerializer"/> class.
        /// </summary>
        public LocalJsonSerializer(BaseOrchestrator orchestrator = null, SyncContext context = null)
        {
            if (orchestrator == null)
                return;

            if (orchestrator.HasInterceptors<DeserializingRowArgs>())
            {

                this.OnReadingRow(async (schemaTable, rowString) =>
                {
                    var args = new DeserializingRowArgs(context, schemaTable, rowString);
                    await orchestrator.InterceptAsync(args).ConfigureAwait(false);
                    return args.Result;
                });
            }

            if (orchestrator.HasInterceptors<SerializingRowArgs>())
            {

                this.OnWritingRow(async (schemaTable, rowArray) =>
                {
                    var args = new SerializingRowArgs(context, schemaTable, rowArray);
                    await orchestrator.InterceptAsync(args).ConfigureAwait(false);
                    return args.Result;
                });
            }
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="LocalJsonSerializer"/> class.
        /// </summary>
        ~LocalJsonSerializer()
        {
            this.Dispose(false);
        }

        /// <summary>
        /// Gets the file extension.
        /// </summary>
        public static string Extension => "json";

        /// <summary>
        /// Gets or sets a value indicating whether returns if the file is opened.
        /// </summary>
        public bool IsOpen
        {
            get => Interlocked.CompareExchange(ref this.isOpen, 0, 0) == 1;
            set => Interlocked.Exchange(ref this.isOpen, value ? 1 : 0);
        }

        /// <summary>
        /// Get the table contained in a serialized file.
        /// </summary>
        public static (SyncTable SchemaTable, int RowsCount, SyncRowState State) GetSchemaTableFromFile(string path)
        {
            if (!File.Exists(path))
                return default;

            string tableName = null, schemaName = null;
            var rowsCount = 0;

            SyncTable schemaTable = null;
            var state = SyncRowState.None;

            using var fileStream = File.OpenRead(path);
            using var jsonReader = new JsonReader(fileStream);

            while (jsonReader.Read())
            {
                if (jsonReader.TokenType != JsonTokenType.PropertyName)
                    continue;

                // read current value
                var propertyValue = jsonReader.GetString();

                switch (propertyValue)
                {
                    case "n":
                        tableName = jsonReader.ReadAsString();
                        break;
                    case "s":
                        schemaName = jsonReader.ReadAsString();
                        break;
                    case "st":
                        state = (SyncRowState)jsonReader.ReadAsInt32();
                        break;
                    case "c": // Dont want to read columns if any
                        schemaTable = GetSchemaTableFromReader(jsonReader, tableName, schemaName);
                        break;
                    case "r":
                        // go into first array
                        var hasToken = jsonReader.Read();

                        if (!hasToken)
                            break;

                        var depth = jsonReader.Depth;

                        // iterate objects array
                        while (jsonReader.Read() && jsonReader.Depth > depth)
                        {
                            var innerDepth = jsonReader.Depth;

                            // iterate values
                            while (jsonReader.Read() && jsonReader.Depth > innerDepth)
                                continue;

                            rowsCount++;
                        }

                        break;
                    default:
                        break;
                }
            }

            return (schemaTable, rowsCount, state);
        }

        /// <summary>
        /// Close the current file, close the writer.
        /// </summary>
        public void CloseFile()
        {
            if (!this.IsOpen)
                return;

            this.writerLock.Wait();

            try
            {
                if (this.writer != null)
                {
                    this.writer.WriteEndArray();
                    this.writer.WriteEndObject();
                    this.writer.WriteEndArray();
                    this.writer.WriteEndObject();
                    this.writer.Flush();
                    this.writer.Dispose();
                }

                this.sw?.Dispose();

                // Tide fork: atomic publish of the temp file to its real target.
                // Done after the writer/StreamWriter have flushed and released the handle.
                // PromoteTempFile clears targetPath/tempPath on success so the orphan
                // cleanup below is a no-op when everything ran to completion.
                this.PromoteTempFile();

                this.IsOpen = false;
            }
            finally
            {
                // Tide fork: if any step above threw (e.g. SIGKILL handler, IO error),
                // an orphan .tide-tmp would otherwise be left on disk forever. Delete it.
                this.CleanupOrphanTempFile();
                this.writerLock.Release();
            }
        }

        /// <summary>
        /// Close the current file, close the writer.
        /// </summary>
        public async Task CloseFileAsync()
        {
            if (!this.IsOpen)
                return;

            await this.writerLock.WaitAsync().ConfigureAwait(false);

            try
            {
                if (this.writer != null)
                {
                    this.writer.WriteEndArray();
                    this.writer.WriteEndObject();
                    this.writer.WriteEndArray();
                    this.writer.WriteEndObject();
                    await this.writer.FlushAsync().ConfigureAwait(false);
                    await this.writer.DisposeAsync().ConfigureAwait(false);
                }

#if NET6_0_OR_GREATER
                await this.sw.DisposeAsync().ConfigureAwait(false);
#else
                this.sw?.Dispose();
#endif

                // Tide fork: atomic publish of the temp file to its real target.
                // Done after the writer/StreamWriter have flushed and released the handle.
                // PromoteTempFile clears targetPath/tempPath on success so the orphan
                // cleanup below is a no-op when everything ran to completion.
                this.PromoteTempFile();

                this.IsOpen = false;
            }
            finally
            {
                // Tide fork: orphan-temp cleanup. See sibling CloseFile() for rationale.
                this.CleanupOrphanTempFile();
                this.writerLock.Release();
            }
        }

        /// <summary>
        /// Open the file and write header.
        /// </summary>
        public async Task OpenFileAsync(string path, SyncTable schemaTable, SyncRowState state, bool append = false)
        {
            Guard.ThrowIfNull(schemaTable);
            Guard.ThrowIfNullOrEmpty(path);

            await this.ResetWriterAsync().ConfigureAwait(false);

            // Tide fork: if a previous open cycle on this instance left a temp file
            // behind (e.g. caller re-opened without closing), discard it before
            // starting a new one.
            this.CleanupOrphanTempFile();

            this.IsOpen = true;

            var fi = new FileInfo(path);

            if (!fi.Directory.Exists)
                fi.Directory.Create();

            await this.writerLock.WaitAsync().ConfigureAwait(false);

            try
            {
                // Tide fork: for non-append writes, stream to a sibling temp file and
                // atomically rename in CloseFileAsync. Append writes are left untouched
                // because callers rely on extending an existing valid file.
                if (!append)
                {
                    this.targetPath = path;
                    this.tempPath = path + "." + Guid.NewGuid().ToString("N") + ".tide-tmp";
                    this.sw = new StreamWriter(this.tempPath, append: false);
                }
                else
                {
                    this.targetPath = null;
                    this.tempPath = null;
                    this.sw = new StreamWriter(path, append);
                }

                this.writer = new Utf8JsonWriter(this.sw.BaseStream);

                this.writer.WriteStartObject();
                this.writer.WritePropertyName("t");

                this.writer.WriteStartArray();
                this.writer.WriteStartObject();

                this.writer.WriteString("n", schemaTable.TableName);
                this.writer.WriteString("s", schemaTable.SchemaName);
                this.writer.WriteNumber("st", (int)state);

                this.writer.WriteStartArray("c");
                foreach (var c in schemaTable.Columns)
                {
                    this.writer.WriteStartObject();
                    this.writer.WriteString("n", c.ColumnName);
                    this.writer.WriteString("t", c.DataType);
                    if (schemaTable.IsPrimaryKey(c.ColumnName))
                    {
                        this.writer.WriteNumber("p", 1);
                    }

                    this.writer.WriteEndObject();
                }

                this.writer.WriteEndArray();
                this.writer.WriteStartArray("r");

                await this.writer.FlushAsync().ConfigureAwait(false);
            }
            finally
            {
                this.writerLock.Release();
            }
        }

        /// <summary>
        /// Append a sync row to the writer.
        /// </summary>
        public async Task WriteRowToFileAsync(SyncRow row, SyncTable schemaTable)
        {
            Guard.ThrowIfNull(row);

            var innerRow = row.ToArray();

            string str;

            if (this.writingRowAsync != null)
                str = await this.writingRowAsync(schemaTable, innerRow).ConfigureAwait(false);
            else
                str = string.Empty; // This won't ever be used, but is need to compile.

            await this.writerLock.WaitAsync().ConfigureAwait(false);

            try
            {
                this.writer.WriteStartArray();

                if (this.writingRowAsync != null)
                {
                    this.writer.WriteStringValue(str);
                }
                else
                {
                    for (var i = 0; i < innerRow.Length; i++)
                        this.writer.WriteRawValue(Serializer.Serialize(innerRow[i]));
                }

                this.writer.WriteEndArray();
                await this.writer.FlushAsync().ConfigureAwait(false);
            }
            finally
            {
                this.writerLock.Release();
            }
        }

        /// <summary>
        /// Interceptor on writing row.
        /// </summary>
        public void OnWritingRow(Func<SyncTable, object[], Task<string>> func) => this.writingRowAsync = func;

        /// <summary>
        /// Interceptor on reading row.
        /// </summary>
        public void OnReadingRow(Func<SyncTable, string, Task<object[]>> func) => this.readingRowAsync = func;

        /// <summary>
        /// Gets the current file size.
        /// </summary>
        /// <returns>Current file size as long.</returns>
        public async Task<long> GetCurrentFileSizeAsync()
        {
            var position = 0L;

            await this.writerLock.WaitAsync().ConfigureAwait(false);

            try
            {
                if (this.sw?.BaseStream != null)
                {
                    position = this.sw.BaseStream.Position / 1024L;
                }
            }
            finally
            {
                this.writerLock.Release();
            }

            return position;
        }

        /// <summary>
        /// Enumerate all rows from file.
        /// </summary>
        public IEnumerable<SyncRow> GetRowsFromFile(string path, SyncTable schemaTable)
        {
            if (!File.Exists(path))
                yield break;

            using var stream = File.OpenRead(path);
            using var jsonReader = new JsonReader(stream);

            var state = SyncRowState.None;

            string tableName = null, schemaName = null;

            while (jsonReader.Read())
            {
                if (jsonReader.TokenType != JsonTokenType.PropertyName)
                    continue;

                var propertyValue = jsonReader.GetString();

                switch (propertyValue)
                {
                    case "n":
                        tableName = jsonReader.ReadAsString();
                        break;
                    case "s":
                        schemaName = jsonReader.ReadAsString();
                        break;
                    case "st":
                        state = (SyncRowState)jsonReader.ReadAsInt16();
                        break;
                    case "c":
                        var tmpTable = GetSchemaTableFromReader(jsonReader, schemaTable?.TableName ?? tableName, schemaTable?.SchemaName ?? schemaName);

                        if (tmpTable != null)
                            schemaTable = tmpTable;

                        continue;
                    case "r":

                        var schemaEmpty = schemaTable == null;

                        if (schemaEmpty)
                            schemaTable = new SyncTable(tableName, schemaName);

                        // go into first array
                        var hasToken = jsonReader.Read();

                        if (!hasToken)
                            break;

                        var depth = jsonReader.Depth;

                        // iterate objects array
                        while (jsonReader.Read() && jsonReader.Depth > depth)
                        {
                            var innerDepth = jsonReader.Depth;

                            // iterate values
                            var index = 0;
                            var values = new object[schemaTable.Columns.Count + 1];
                            var stringBuilder = new StringBuilder();
                            var getStringOnly = this.readingRowAsync != null;

                            while (jsonReader.Read() && jsonReader.TokenType != JsonTokenType.EndArray)
                            {
                                object value = null;
                                var columnType = index >= 1 ? schemaTable.Columns[index - 1].GetDataType() : typeof(short);

                                if (this.readingRowAsync != null)
                                {
                                    if (index > 0)
                                        stringBuilder.Append(',');

                                    stringBuilder.Append(jsonReader.GetString());
                                }
                                else
                                {
                                    if (jsonReader.TokenType is JsonTokenType.Null or JsonTokenType.None)
                                        value = null;
                                    else if (jsonReader.TokenType == JsonTokenType.String && jsonReader.TryGetDateTimeOffset(out var datetimeOffset))
                                        value = datetimeOffset;
                                    else if (jsonReader.TokenType == JsonTokenType.String)
                                        value = jsonReader.GetString();
                                    else if (jsonReader.TokenType is JsonTokenType.False or JsonTokenType.True)
                                        value = jsonReader.GetBoolean();
                                    else if (jsonReader.TokenType == JsonTokenType.Number && jsonReader.TryGetInt64(out var l))
                                        value = l;
                                    else if (jsonReader.TokenType == JsonTokenType.Number)
                                        value = jsonReader.GetDouble();

                                    try
                                    {
                                        if (value != null)
                                            values[index] = SyncTypeConverter.TryConvertTo(value, columnType);
                                    }
                                    catch (Exception)
                                    {
                                        // No exception as a custom converter could be used to override type
                                        // like a datetime converted to ticks (long)
                                    }
                                }

                                index++;
                            }

                            if (this.readingRowAsync != null)
                                values = this.readingRowAsync(schemaTable, stringBuilder.ToString()).GetAwaiter().GetResult();

                            if (values == null || values.Length < 2)
                            {
                                var rowStr = "[" + string.Join(",", values) + "]";
                                throw new Exception($"Can't read row {rowStr} from file {path}");
                            }

                            if (schemaEmpty) // array[0] contains the state, not a column
                            {
                                for (var i = 1; i < values.Length; i++)
                                    schemaTable.Columns.Add($"C{i}", values[i].GetType());

                                schemaEmpty = false;
                            }

                            if (values.Length != (schemaTable.Columns.Count + 1))
                            {
                                var rowStr = "[" + string.Join(",", values) + "]";
                                throw new Exception($"Table {schemaTable.GetFullName()} with {schemaTable.Columns.Count} columns does not have the same columns count as the row read {rowStr} which have {values.Length - 1} values.");
                            }

                            // if we have some columns, we check the date time thing
                            if (schemaTable.Columns?.HasSyncColumnOfType(typeof(DateTime)) == true)
                            {
                                for (var index2 = 1; index2 < values.Length; index2++)
                                {
                                    var column = schemaTable.Columns[index2 - 1];

                                    // Set the correct value in existing row for DateTime types.
                                    // They are being Deserialized as DateTimeOffsets
                                    if (column != null && column.GetDataType() == typeof(DateTime) && values[index2] != null && values[index2] is DateTimeOffset)
                                        values[index2] = ((DateTimeOffset)values[index2]).DateTime;
                                }
                            }

                            yield return new SyncRow(schemaTable, values);
                        }

                        yield break;
                    default:
                        break;
                }
            }
        }

        /// <summary>
        /// Dispose the current instance.
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            this.Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose the current instance.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            await this.CloseFileAsync().ConfigureAwait(false);
            this.writerLock.Dispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose the current instance.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    this.CloseFile();
                    this.writerLock?.Dispose();
                }

                // Tide fork: final safety net for orphan temp files. If CloseFile()
                // didn't run (IsOpen was false because open threw before we set it,
                // or someone disposed before opening) but a temp path was recorded,
                // delete it. Also runs from the finalizer (disposing == false) so a
                // leaked instance still cleans up its disk artefact.
                this.CleanupOrphanTempFile();

                this.disposedValue = true;
            }
        }

        /// <summary>
        /// Tide fork: atomically promote the .tide-tmp file produced during the
        /// current open/write cycle to its target path. No-op when there is no
        /// temp file (append-mode writes, or already promoted).
        /// </summary>
        private void PromoteTempFile()
        {
            var target = this.targetPath;
            var temp = this.tempPath;

            // Clear state up front so a second call (e.g. CloseFile -> Dispose ->
            // CloseFile) is a no-op and so the orphan-cleanup path below knows
            // there is nothing to clean.
            this.targetPath = null;
            this.tempPath = null;

            if (string.IsNullOrEmpty(temp) || string.IsNullOrEmpty(target))
                return;

            if (!File.Exists(temp))
                return;

#if NETSTANDARD2_0
            // File.Move(string, string, bool) is unavailable on netstandard2.0.
            // Best-effort: delete the target if present, then rename. Two-step,
            // so not strictly atomic on this TFM — but only used by callers
            // running on .NET Framework, which is not the production target.
            if (File.Exists(target))
                File.Delete(target);
            File.Move(temp, target);
#else
            File.Move(temp, target, overwrite: true);
#endif
        }

        /// <summary>
        /// Tide fork: delete any orphaned .tide-tmp file recorded on this
        /// instance. Called from finally blocks and Dispose paths so a failed
        /// open/close never leaves stale files on disk. Safe to call repeatedly.
        /// </summary>
        private void CleanupOrphanTempFile()
        {
            var temp = this.tempPath;
            this.tempPath = null;
            this.targetPath = null;

            if (string.IsNullOrEmpty(temp))
                return;

            try
            {
                if (File.Exists(temp))
                    File.Delete(temp);
            }
            catch
            {
                // Swallow: cleanup is best-effort, never raise from a finally
                // block or finaliser. A stale .tide-tmp is recoverable; an
                // exception escaping Dispose is not.
            }
        }

        private static SyncTable GetSchemaTableFromReader(JsonReader jsonReader, string tableName, string schemaName)
        {
            bool hadMoreTokens;

            while ((hadMoreTokens = jsonReader.Read()) && jsonReader.TokenType != JsonTokenType.StartArray)
                continue;

            if (!hadMoreTokens)
                return null;

            // get current depth
            var schemaTable = new SyncTable(tableName, schemaName);

            while (jsonReader.Read() && jsonReader.TokenType != JsonTokenType.EndArray)
            {
                // reading an object containing a column
                if (jsonReader.TokenType == JsonTokenType.StartObject)
                {
                    string includedColumnName = null;
                    string includedColumnTypeName = null;
                    var isPrimaryKey = false;

                    while (jsonReader.Read() && jsonReader.TokenType != JsonTokenType.EndObject)
                    {
                        var propertyValue = jsonReader.GetString();

                        switch (propertyValue)
                        {
                            case "n":
                                includedColumnName = jsonReader.ReadAsString();
                                break;
                            case "t":
                                includedColumnTypeName = jsonReader.ReadAsString();
                                break;
                            case "p":
                                isPrimaryKey = jsonReader.ReadAsInt16() == 1;
                                break;
                            default:
                                break;
                        }
                    }

                    var includedColumnType = SyncColumn.GetTypeFromAssemblyQualifiedName(includedColumnTypeName);

                    // Adding the column
                    if (!string.IsNullOrEmpty(includedColumnName) && !string.IsNullOrEmpty(includedColumnTypeName))
                    {
                        schemaTable.Columns.Add(new SyncColumn(includedColumnName, includedColumnType));

                        if (isPrimaryKey)
                            schemaTable.PrimaryKeys.Add(includedColumnName);
                    }
                }
            }

            return schemaTable;
        }

        private async Task ResetWriterAsync()
        {
            if (this.writer == null)
                return;

            await this.writerLock.WaitAsync().ConfigureAwait(false);

            try
            {
                await this.writer.DisposeAsync().ConfigureAwait(false);
                this.writer = null;
            }
            finally
            {
                this.writerLock.Release();
            }
        }
    }
}