using System;
using System.Linq;
using System.Threading.Tasks;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;

namespace BigtableClientMocking.Tests
{
    public class ClassUnderTest
    {
        private const string BigtableProjectId = "project1";

        private readonly IBigtableClientAdapter _bigtableClientAdapter;

        private readonly string _bigtableInstanceId;

        public ClassUnderTest(IBigtableClientAdapter bigtableClientAdapter, string bigtableInstanceId)
        {
            _bigtableClientAdapter = bigtableClientAdapter;
            _bigtableInstanceId = bigtableInstanceId;
        }

        public async Task<SomeItem[]> GetSomeItems(string tableName, string key, DateTime endDate)
        {
            TableName tableNameInstance = new TableName(BigtableProjectId, _bigtableInstanceId, tableName);

            RowSet rowSet = RowSet.FromRowKey(key);

            RowFilter filter = RowFilters.Chain(
                RowFilters.TimestampRange(null, endDate),
                RowFilters.CellsPerColumnLimit(1));

            return await _bigtableClientAdapter.ReadRows(tableNameInstance, rowSet, filter)
                .Select(x => new SomeItem
                {
                    Key = x.Key.ToStringUtf8(),
                    Name = x.Families.First(x => x.Name == "family1").Columns[0].Cells[0].Value.ToStringUtf8()
                })
                .ToArrayAsync();
        }
    }
}
