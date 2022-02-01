using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;
using Moq;
using NUnit.Framework;

namespace BigtableClientMocking.Tests
{
    [TestFixture]
    public class Tests
    {
        [Test]
        public async Task RegularTest()
        {
            DateTime endDate = DateTime.UtcNow;

            var bigtableClientAdapterMock = new Mock<IBigtableClientAdapter>(MockBehavior.Strict);
            var sut = new ClassUnderTest(bigtableClientAdapterMock.Object);

            TableName tableName = new TableName("proj", "inst", "tabl");

            RowSet rowSet = RowSet.FromRowKeys("row1", "row2");

            RowFilter filter = RowFilters.Chain(
                RowFilters.TimestampRange(null, endDate),
                RowFilters.CellsPerColumnLimit(1));

            List<Row> setupRows = new List<Row>();
            Row row1 = new Row();
            setupRows.Add(row1);
            row1.Key = ByteString.CopyFromUtf8("key1");

            Family family = new Family { Name = "family1" };
            row1.Families.Add(family);
            Column column = new Column { Qualifier = ByteString.CopyFromUtf8("col1") };
            column.Cells.Add(new Cell { Value = ByteString.CopyFromUtf8("name1") });
            family.Columns.Add(column);

            bigtableClientAdapterMock.Setup(x => x.ReadRows(tableName, rowSet, filter, null, null))
                .Returns(setupRows.ToAsyncEnumerable());

            var result = await sut.GetSomeItems(endDate);
            result.Should().BeEquivalentTo(new[]
            {
                new SomeItem { Key = "key1", Name = "name1" }
            });

            bigtableClientAdapterMock.VerifyAll();
        }

        [Test]
        public async Task SimplifiedTest()
        {
            DateTime endDate = DateTime.UtcNow;

            var bigtableClientAdapterMock = new Mock<IBigtableClientAdapter>(MockBehavior.Strict);
            var sut = new ClassUnderTest(bigtableClientAdapterMock.Object);

            TableName tableName = new TableName("proj", "inst", "tabl");

            RowSet rowSet = RowSet.FromRowKeys("row1", "row2");

            RowFilter filter = RowFilters.Chain(
                RowFilters.TimestampRange(null, endDate),
                RowFilters.CellsPerColumnLimit(1));

            Row[] setupRows = new Row[]
            {
                new RowBuilder("key1")
                    .AddCellValue("family1", "col1", "name1")
            };

            bigtableClientAdapterMock.Setup(x => x.ReadRows(tableName, rowSet, filter, null, null))
                .Returns(setupRows.ToAsyncEnumerable());

            var result = await sut.GetSomeItems(endDate);
            result.Should().BeEquivalentTo(new[]
            {
                new SomeItem { Key = "key1", Name = "name1" }
            });

            bigtableClientAdapterMock.VerifyAll();
        }
    }
}
