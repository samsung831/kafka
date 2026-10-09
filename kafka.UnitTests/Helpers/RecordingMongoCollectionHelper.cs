using System.Reflection;
using kafka.Shared.Models.Accounts;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;

namespace kafka.UnitTests.Helpers;

public class RecordingMongoCollectionHelper : DispatchProxy
{
    #region Properties

    #region Public

    #region Stored
    public AccountDocument? Stored { get; set; }
    #endregion

    #region ModifiedCount
    public long ModifiedCount { get; set; }
    #endregion

    #region RecoveryFailure
    public Exception? RecoveryFailure { get; set; }
    #endregion

    #region Writes
    public List<(FilterDefinition<AccountDocument> Filter, AccountDocument Document, ReplaceOptions Options, CancellationToken Token)> Writes { get; } = new();
    #endregion

    #region DuplicateKey
    public MongoWriteException DuplicateKey { get; } = new(
        new ConnectionId(new ServerId(new ClusterId(), new System.Net.DnsEndPoint("localhost", 27017))),
        (WriteError)Activator.CreateInstance(typeof(WriteError), BindingFlags.Instance | BindingFlags.NonPublic,
            null, [ServerErrorCategory.DuplicateKey, 11000, "Duplicate key", new BsonDocument()], null)!, null, null);
    #endregion

    #endregion

    #endregion

    #region Methods

    #region Protected

    #region Invoke
    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        if (targetMethod!.Name == "ReplaceOneAsync" && args!.Length == 4)
        {
            Writes.Add(((FilterDefinition<AccountDocument>)args[0]!, (AccountDocument)args[1]!,
                (ReplaceOptions)args[2]!, (CancellationToken)args[3]!));
            if (Writes.Count == 1)
            {
                return Task.FromException<ReplaceOneResult>(DuplicateKey);
            }

            return RecoveryFailure is not null
                ? Task.FromException<ReplaceOneResult>(RecoveryFailure)
                : Task.FromResult<ReplaceOneResult>(new ReplaceOneResult.Acknowledged(ModifiedCount, ModifiedCount, null));
        }

        if (targetMethod.Name == "FindAsync")
        {
            return Task.FromResult<IAsyncCursor<AccountDocument>>(new SingleBatchCursor(Stored));
        }

        throw new NotSupportedException(targetMethod.Name);
    }
    #endregion

    #endregion

    #endregion

    private sealed class SingleBatchCursor(AccountDocument? document) : IAsyncCursor<AccountDocument>
    {
        #region Properties

        #region Private

        #region Read
        private bool _read;
        #endregion

        #endregion

        #region Public

        #region Current
        public IEnumerable<AccountDocument> Current => document is null ? [] : [document];
        #endregion

        #endregion

        #endregion

        #region Methods

        #region Public

        #region MoveNext
        public bool MoveNext(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var available = !_read;
            _read = true;
            return available;
        }
        #endregion

        #region MoveNextAsync
        public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default) => Task.FromResult(MoveNext(cancellationToken));
        #endregion

        #region Dispose
        public void Dispose() { }
        #endregion

        #endregion

        #endregion
    }
}
