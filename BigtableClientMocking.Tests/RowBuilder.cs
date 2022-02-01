using System.Linq;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;

namespace BigtableClientMocking.Tests
{
    public class RowBuilder
    {
        public RowBuilder(string key)
        {
            Row.Key = ByteString.CopyFromUtf8(key);
        }

        public Row Row { get; set; } = new Row();

        public RowBuilder AddCellValue(string familyName, string columnQualifier, object value)
        {
            Family family = Row.Families.FirstOrDefault(x => x.Name == familyName);

            if (family is null)
            {
                family = new Family { Name = familyName };
                Row.Families.Add(family);
            }

            ByteString columnQualifierAsByteString = ByteString.CopyFromUtf8(columnQualifier);
            Column column = family.Columns.FirstOrDefault(x => x.Qualifier == columnQualifierAsByteString);

            if (column is null)
            {
                column = new Column { Qualifier = columnQualifierAsByteString };
                family.Columns.Add(column);
            }

            string valueAsString = value?.ToString();
            column.Cells.Add(new Cell { Value = ByteString.CopyFromUtf8(valueAsString) });


            return this;
        }

        public static implicit operator Row(RowBuilder builder) => builder.Row;
    }
}
