using System;
using System.Threading.Tasks;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Containers.Builders;
using DotNet.Testcontainers.Containers.Modules;
using DotNet.Testcontainers.Containers.WaitStrategies;
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

        private IDockerContainer _cloudSdkContainer;

        private BigtableClient _bigtableClient;

        private BigtableTableAdminClient _bigtableTableAdminClient;

        private TableName _bigtableTableName;

        private ClassUnderTest _sut;

        [OneTimeSetUp]
        public async Task SetUpFixture()
        {
            var containerBuilder = new TestcontainersBuilder<TestcontainersContainer>()
                .WithImage("gcr.io/google.com/cloudsdktool/cloud-sdk:emulators")
                .WithName("cloud-sdk-emulators")
                .WithPortBinding(8086, 8086)
                .WithExposedPort(8086)
                .WithCommand("/bin/sh", "-c", "gcloud beta emulators bigtable start --host-port=0.0.0.0:8086")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(8086));

            _cloudSdkContainer = containerBuilder.Build();
            await _cloudSdkContainer.StartAsync();
        }

        [OneTimeTearDown]
        public async Task TearDownFixture()
        {
            if (_cloudSdkContainer != null)
                await _cloudSdkContainer.DisposeAsync();
        }

        [SetUp]
        public void SetUp()
        {
            string _bigtableInstanceId = Guid.NewGuid().ToString();

            var callInvoker = new GcpCallInvoker(
                "localhost:8086",
                ChannelCredentials.Insecure,
                GrpcCoreAdapter.Instance.ConvertOptions(BigtableServiceApiSettings.GetDefault().CreateChannelOptions()));

            _bigtableClient = new BigtableClientBuilder { CallInvoker = callInvoker }
                .Build();

            _bigtableTableAdminClient = new BigtableTableAdminClientBuilder { CallInvoker = callInvoker }
                .Build();

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

            _bigtableTableName = new TableName(BigtableProjectId, _bigtableInstanceId, BigtableTableId);

            _sut = new ClassUnderTest(new BigtableClientAdapter(_bigtableClient), _bigtableInstanceId);
        }

        [TearDown]
        public void TearDown()
        {
            if (_bigtableTableName != null)
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
