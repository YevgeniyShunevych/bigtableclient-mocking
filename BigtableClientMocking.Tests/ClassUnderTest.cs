using System;
using System.Linq;
using System.Threading.Tasks;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;

namespace BigtableClientMocking.Tests
{
    public class ClassUnderTest
    {
        private readonly IBigtableClientAdapter _bigbableClientAdapter;

        public ClassUnderTest(IBigtableClientAdapter bigtableClientAdapter)
        {
            _bigbableClientAdapter = bigtableClientAdapter;
        }

        public async Task<SomeItem[]> GetSomeItems(DateTime endDate)
        {
            TableName tableName = new TableName("proj", "inst", "tabl");

            string[] rowKeys = new[] { "row1", "row2" };
            RowSet rowSet = RowSet.FromRowKeys(rowKeys.Select(x => new BigtableByteString(x)));

            RowFilter filter = RowFilters.Chain(
                RowFilters.TimestampRange(null, endDate),
                RowFilters.CellsPerColumnLimit(1));

            return await _bigbableClientAdapter.ReadRows(tableName, rowSet, filter)
                .Select(x => new SomeItem
                {
                    Key = x.Key.ToStringUtf8(),
                    Name = x.Families.First(x => x.Name == "family1").Columns[0].Cells[0].Value.ToStringUtf8()
                })
                .ToArrayAsync();
        }
    }
}
