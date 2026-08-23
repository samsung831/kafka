using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.IntegrationTests.Infrastructure;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class IntegrationTestCollection : ICollectionFixture<IntegrationTestFixture>
{
    #region Properties

    #region Public
    public const string Name = "Kafka and MongoDB integration tests";
    #endregion

    #endregion
}
