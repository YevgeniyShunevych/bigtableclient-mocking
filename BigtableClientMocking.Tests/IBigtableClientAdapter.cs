using System.Collections.Generic;
using Google.Api.Gax.Grpc;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;

namespace BigTableClientMocking.Tests
{
    public interface IBigtableClientAdapter
    {
        IAsyncEnumerable<Row> ReadRows(TableName tableName, RowSet rows = null, RowFilter filter = null, long? rowsLimit = null, CallSettings callSettings = null);
    }
}
