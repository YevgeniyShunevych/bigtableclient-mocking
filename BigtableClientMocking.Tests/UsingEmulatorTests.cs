using System;
using System.Threading.Tasks;
using FluentAssertions;
using Google.Api.Gax.Grpc.GrpcCore;
using Google.Cloud.Bigtable.Admin.V2;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Grpc.Core;
using Grpc.Gcp;
using NUnit.Framework;

namespace BigtableClientMocking.Tests
{
    [TestFixture]
    public class UsingEmulatorTests
    {
        private const string BigtableProjectId = "project1";

        private const string BigtableTableId = "table1";

        private BigtableClient _bigtableClient;

        private BigtableTableAdminClient _bigtableTableAdminClient;

        private TableName _bigtableTableName;

        private ClassUnderTest _sut;

        [SetUp]
        public void SetUp()
        {
            string _bigtableInstanceId = Guid.NewGuid().ToString();

            var invoker = new GcpCallInvoker("localhost:8086", ChannelCredentials.Insecure, GrpcCoreAdapter.Instance.ConvertOptions(BigtableServiceApiSettings.GetDefault().CreateChannelOptions()));

            _bigtableClient = new BigtableClientBuilder { CallInvoker = invoker }
                .Build();

            _bigtableTableAdminClient = new BigtableTableAdminClientBuilder { CallInvoker = invoker }
                .Build();

            _bigtableTableName = new TableName(BigtableProjectId, _bigtableInstanceId, BigtableTableId);

            _bigtableTableAdminClient.CreateTable(
                new InstanceName(BigtableProjectId, _bigtableInstanceId),
                BigtableTableId,
                new Table
                {
                    Granularity = Table.Types.TimestampGranularity.Millis,
                    ColumnFamilies =
                    {
                        ["family1"] = new ColumnFamily
                        {
                            GcRule = new GcRule
                            {
                                MaxNumVersions = 1
                            }
                        }
                    }
                });

            _sut = new ClassUnderTest(new BigtableClientAdapter(_bigtableClient), _bigtableInstanceId);
        }

        [TearDown]
        public void TearDown()
        {
            _bigtableTableAdminClient.DeleteTable(_bigtableTableName);
        }

        [Test]
        public async Task Test()
        {
            _bigtableClient.MutateRow(
                _bigtableTableName,
                new BigtableByteString("key1"),
                Mutations.SetCell("family1", "col1", new BigtableByteString("val1"), new BigtableVersion(DateTime.UtcNow)));

            var result = await _sut.GetSomeItems(BigtableTableId, "key1", DateTime.UtcNow);
            result.Should().BeEquivalentTo(new[]
            {
                new SomeItem { Key = "key1", Name = "val1" }
            });
        }
    }
}
