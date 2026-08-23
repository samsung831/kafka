using kafka.Shared.Models.Common;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Servers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Channels;

namespace kafka.Shared.MongoDB;

public sealed class VersionedDocumentWriter<TDocument> where TDocument : BaseDocument
{
    #region Constructor
    public VersionedDocumentWriter(IMongoCollection<TDocument> collection)
    {
        _collection = collection;
    }
    #endregion

    #region Properties

    #region Private
    private readonly IMongoCollection<TDocument> _collection;
    #endregion

    #endregion

    #region Methods

    #region Public

    #region UpsertAsync
    /// <summary>
    /// Upserts a document into the MongoDB collection. If a document with the same Id exists and has a lower version, it will be replaced.
    /// If the document does not exist, it will be inserted. If the existing document has an equal or higher version, the operation will be ignored.
    /// </summary>
    /// <param name="document">The document to upsert.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="VersionedWriteResult"/> indicating the outcome of the operation.</returns>
    /// <exception cref="ArgumentException">Thrown when the document Id is null or empty, or the document version is negative.</exception>
    public async Task<VersionedWriteResult> UpsertAsync(TDocument document, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(document);

        if (string.IsNullOrWhiteSpace(document.Id))
        {
            throw new ArgumentException("Document _id is required.", nameof(document));
        }

        if (document.Version < 0)
        {
            throw new ArgumentException("Document version cannot be negative.", nameof(document));
        }

        var filter = Builders<TDocument>.Filter.Eq(
                        item => item.Id,
                        document.Id)
                    &
                    Builders<TDocument>.Filter.Lt(
                        item => item.Version,
                        document.Version);

        try
        {
            var result = await _collection.ReplaceOneAsync(filter, document,
                new ReplaceOptions
                {
                    IsUpsert = true
                },
                cancellationToken);

            if (result.UpsertedId is not null)
            {
                return VersionedWriteResult.Inserted;
            }

            if (result.ModifiedCount > 0)
            {
                return VersionedWriteResult.Updated;
            }

            return VersionedWriteResult.Ignored;
        }
        catch (MongoWriteException exception) when (exception.WriteError?.Category == ServerErrorCategory.DuplicateKey)
        {
            var storedDocument = await _collection.Find(item => item.Id == document.Id).FirstOrDefaultAsync(cancellationToken);

            if (storedDocument is not null && storedDocument.Version >= document.Version)
            {
                return VersionedWriteResult.Ignored;
            }

            throw;
        }
    }
    #endregion

    #endregion

    #endregion
}

public enum VersionedWriteResult
{
    Inserted,
    Updated,
    Ignored
}