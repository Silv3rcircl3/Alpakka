//-----------------------------------------------------------------------
// <copyright file="DirectoryChangesSourceTest.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using Akka.Streams.Alpakka.File.Dsl;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Alpakka.File.Tests
{
    public class DirectoryChangesSourceTest : Akka.TestKit.Xunit2.TestKit
    {
        private readonly string _path;

        public DirectoryChangesSourceTest(ITestOutputHelper helper) : base(output: helper)
        {
            Materializer = Sys.Materializer();

            _path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(_path);
        }

        private ActorMaterializer Materializer { get; }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            Directory.Delete(_path, true);
        }

        [Fact]
        public void A_DirectorySource_should_emit_on_directory_changes()
        {
            var probe = this.CreateSubscriberProbe<Tuple<string, WatcherChangeTypes>>();

            DirectoryChangesSource.Create(_path, 200).RunWith(Sink.FromSubscriber(probe), Materializer);

            probe.Request(1);

            var createdFile = Path.Combine(_path, "testfile1.sample");
            System.IO.File.WriteAllText(createdFile, string.Empty);
            
            var change = probe.ExpectNext();
            Assert.Equal(change.Item1, createdFile);
            Assert.True(change.Item2 == WatcherChangeTypes.Created);

            System.IO.File.WriteAllText(createdFile, "hello");
            change = probe.RequestNext();
            Assert.Equal(change.Item1, createdFile);
            Assert.True(change.Item2 == WatcherChangeTypes.Changed);
            
            System.IO.File.Delete(createdFile);
            change = probe.RequestNext();
            Assert.Equal(change.Item1, createdFile);
            Assert.True(change.Item2 == WatcherChangeTypes.Changed);
            change = probe.RequestNext();
            Assert.Equal(change.Item1, createdFile);
            Assert.True(change.Item2 == WatcherChangeTypes.Deleted);

            probe.Cancel();
        }
    }
}
