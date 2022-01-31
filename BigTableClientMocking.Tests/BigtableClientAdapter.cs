using System.Collections.Generic;
using Google.Api.Gax.Grpc;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;

namespace BigTableClientMocking.Tests
{
    public class BigtableClientAdapter : IBigtableClientAdapter
    {
        private readonly BigtableClient _bigtableClient;

        public BigtableClientAdapter(BigtableClient bigtableClient)
        {
            _bigtableClient = bigtableClient;
        }

        public IAsyncEnumerable<Row> ReadRows(TableName tableName, RowSet rows = null, RowFilter filter = null, long? rowsLimit = null, CallSettings callSettings = null) =>
            _bigtableClient.ReadRows(tableName, rows, filter, rowsLimit, callSettings);
    }
}
