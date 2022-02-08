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
    public class UsingMockingTests
    {
        [Test]
        public async Task StraightforwardTest()
        {
            DateTime endDate = DateTime.UtcNow;

            var bigtableClientAdapterMock = new Mock<IBigtableClientAdapter>(MockBehavior.Strict);
            var sut = new ClassUnderTest(bigtableClientAdapterMock.Object, "instance1");

            TableName tableName = new TableName("project1", "instance1", "table1");

            RowSet rowSet = RowSet.FromRowKey("key1");

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
            column.Cells.Add(new Cell { Value = ByteString.CopyFromUtf8("val1") });
            family.Columns.Add(column);

            bigtableClientAdapterMock.Setup(x => x.ReadRows(tableName, rowSet, filter, null, null))
                .Returns(setupRows.ToAsyncEnumerable());

            var result = await sut.GetSomeItems("table1", "key1", endDate);
            result.Should().BeEquivalentTo(new[]
            {
                new SomeItem { Key = "key1", Name = "val1" }
            });

            bigtableClientAdapterMock.VerifyAll();
        }

        [Test]
        public async Task SimplifiedTest()
        {
            DateTime endDate = DateTime.UtcNow;

            var bigtableClientAdapterMock = new Mock<IBigtableClientAdapter>(MockBehavior.Strict);
            var sut = new ClassUnderTest(bigtableClientAdapterMock.Object, "instance1");

            TableName tableName = new TableName("project1", "instance1", "table1");

            RowSet rowSet = RowSet.FromRowKey("key1");

            RowFilter filter = RowFilters.Chain(
                RowFilters.TimestampRange(null, endDate),
                RowFilters.CellsPerColumnLimit(1));

            Row[] setupRows = new Row[]
            {
                new RowBuilder("key1")
                    .AddCellValue("family1", "col1", "val1")
            };

            bigtableClientAdapterMock.Setup(x => x.ReadRows(tableName, rowSet, filter, null, null))
                .Returns(setupRows.ToAsyncEnumerable());

            var result = await sut.GetSomeItems("table1", "key1", endDate);
            result.Should().BeEquivalentTo(new[]
            {
                new SomeItem { Key = "key1", Name = "val1" }
            });

            bigtableClientAdapterMock.VerifyAll();
        }
    }
}
