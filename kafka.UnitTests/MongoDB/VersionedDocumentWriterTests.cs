using System.Reflection;
using kafka.Shared.Models.Accounts;
using kafka.Shared.MongoDB;
using kafka.UnitTests.Helpers;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace kafka.UnitTests.MongoDB;

public sealed class VersionedDocumentWriterTests
{
    #region Methods

    #region Private

    #region CreateWriter
    private static (VersionedDocumentWriter<AccountDocument> Writer, RecordingMongoCollectionHelper Proxy) CreateWriter()
    {
        var collection = DispatchProxy.Create<IMongoCollection<AccountDocument>, RecordingMongoCollectionHelper>();
        return (new VersionedDocumentWriter<AccountDocument>(collection), (RecordingMongoCollectionHelper)collection);
    }
    #endregion

    #endregion

    #region Public

    #region UpsertAsync_DuplicateKey_RecoversConditionally
    [Theory]
    [InlineData(1, 1, VersionedWriteResult.Updated, 2)]
    [InlineData(1, 0, VersionedWriteResult.Ignored, 2)]
    [InlineData(2, 0, VersionedWriteResult.Ignored, 1)]
    [InlineData(3, 0, VersionedWriteResult.Ignored, 1)]
    public async Task UpsertAsync_DuplicateKey_RecoversConditionally(long storedVersion, long modifiedCount,
        VersionedWriteResult expected, int expectedWrites)
    {
        var (writer, proxy) = CreateWriter();
        proxy.Stored = new AccountDocument { Version = storedVersion };
        proxy.ModifiedCount = modifiedCount;
        using var cancellation = new CancellationTokenSource();
        var incoming = new AccountDocument { Id = "64c3e0f5d1f4c2a1b2c3d4e5", Version = 2 };

        var result = await writer.UpsertAsync(incoming, cancellation.Token);

        Assert.Equal(expected, result);
        Assert.Equal(expectedWrites, proxy.Writes.Count);
        Assert.True(proxy.Writes[0].Options.IsUpsert);
        if (expectedWrites == 2)
        {
            var recovery = proxy.Writes[1];
            Assert.False(recovery.Options.IsUpsert);
            Assert.Same(proxy.Writes[0].Filter, recovery.Filter);
            Assert.Same(incoming, recovery.Document);
            Assert.Equal(cancellation.Token, recovery.Token);
            var rendered = recovery.Filter.Render(new RenderArgs<AccountDocument>(
                BsonSerializer.LookupSerializer<AccountDocument>(), BsonSerializer.SerializerRegistry));
            Assert.Equal(ObjectId.Parse(incoming.Id), rendered["_id"].AsObjectId);
            Assert.Equal(2, rendered["version"]["$lt"].ToInt64());
        }
    }
    #endregion

    #region UpsertAsync_DuplicateKeyWithoutSameId_Rethrows
    [Fact]
    public async Task UpsertAsync_DuplicateKeyWithoutSameId_Rethrows()
    {
        var (writer, proxy) = CreateWriter();

        var exception = await Assert.ThrowsAsync<MongoWriteException>(() =>
            writer.UpsertAsync(new AccountDocument { Id = "id", Version = 2 }));

        Assert.Same(proxy.DuplicateKey, exception);
        Assert.Single(proxy.Writes);
    }
    #endregion

    #region UpsertAsync_RecoveryWriteFailure_Propagates
    [Fact]
    public async Task UpsertAsync_RecoveryWriteFailure_Propagates()
    {
        var (writer, proxy) = CreateWriter();
        proxy.Stored = new AccountDocument { Version = 1 };
        proxy.RecoveryFailure = proxy.DuplicateKey;

        var exception = await Assert.ThrowsAsync<MongoWriteException>(() =>
            writer.UpsertAsync(new AccountDocument { Id = "id", Version = 2 }));

        Assert.Same(proxy.RecoveryFailure, exception);
        Assert.Equal(2, proxy.Writes.Count);
        Assert.False(proxy.Writes[1].Options.IsUpsert);
    }
    #endregion

    #endregion

    #endregion

}
