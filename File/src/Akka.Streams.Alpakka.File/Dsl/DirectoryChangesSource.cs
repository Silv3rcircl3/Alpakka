//-----------------------------------------------------------------------
// <copyright file="DirectoryChangesSource.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------


using System;
using System.Collections.Generic;
using System.IO;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;

namespace Akka.Streams.Alpakka.File.Dsl
{
    /// <summary>
    /// Watches a file system directory and streams change events from it.
    /// </summary>
    public sealed class DirectoryChangesSource<T> : GraphStage<SourceShape<T>>
    {
        #region Logic

        private sealed class Logic : GraphStageLogic, IOutHandler
        {
            private readonly DirectoryChangesSource<T> _stage;
            private FileSystemWatcher _watcher;
            private readonly Queue<T> _buffer;

            public Logic(DirectoryChangesSource<T> stage) : base(stage.Shape)
            {
                _stage = stage;
                _buffer = new Queue<T>();

                SetHandler(stage.Out, OnPull);
            }

            public override void PreStart()
            {
                _watcher = new FileSystemWatcher(_stage._directoryPath);

                var errorCallback = GetAsyncCallback<Exception>(FailStage);
                _watcher.Error += (_, args) =>
                {
                    TryStopWatcher();
                    errorCallback(args.GetException());
                };

                var eventCallback = GetAsyncCallback<FileSystemEventArgs>(OnEvent);
                _watcher.Created += (_, args) => eventCallback(args);
                _watcher.Changed += (_, args) => eventCallback(args);
                _watcher.Deleted += (_, args) => eventCallback(args);
                _watcher.Renamed += (_, args) => eventCallback(args);

                _watcher.EnableRaisingEvents = true;
            }

            public override void PostStop() => TryStopWatcher();

            private void OnEvent(FileSystemEventArgs e)
            {
                var element = _stage._combiner(e.FullPath, e.ChangeType);
                _buffer.Enqueue(element);

                if (_buffer.Count > _stage._maxBufferSize)
                {
                    TryStopWatcher();

                    FailStage(
                        new BufferOverflowException(
                            $"Max event buffer size {_stage._maxBufferSize} reached for {_stage._directoryPath}"));
                }

                if(IsAvailable(_stage.Out))
                    Push(_stage.Out, _buffer.Dequeue());
            }
            
            private void TryStopWatcher()
            {
                if(_watcher == null)
                    return;

                _watcher.EnableRaisingEvents = false;
                _watcher.Dispose();
                _watcher = null;
            }

            public void OnPull()
            {
                if (_buffer.Count == 0)
                    return;

                var element = _buffer.Dequeue();
                if (element != null)
                    Push(_stage.Out, element);
            }

            public void OnDownstreamFinish() => TryStopWatcher();
        }

        #endregion  

        private readonly string _directoryPath;
        private readonly int _maxBufferSize;
        private readonly Func<string, WatcherChangeTypes, T> _combiner;

        /// <param name="directoryPath">Directory to watch</param>
        /// <param name="maxBufferSize">Maximum number of buffered directory changes before the stage fails</param>
        /// <param name="combiner">A function that combines a path and a <see cref="WatcherChangeTypes"/> into an element that will be emitted downstream</param>
        public DirectoryChangesSource(string directoryPath, int maxBufferSize, Func<string, WatcherChangeTypes, T> combiner)
        {
            _directoryPath = directoryPath;
            _maxBufferSize = maxBufferSize;
            _combiner = combiner;

            Shape = new SourceShape<T>(Out);
        }

        protected override Attributes InitialAttributes { get; } = Attributes.CreateName("DirectoryChangesSource");

        public Outlet<T> Out { get; } = new Outlet<T>("DirectoryChangesSource.out");

        public override SourceShape<T> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        {
            if (!Directory.Exists(_directoryPath))
                throw new ArgumentException($"The path {_directoryPath} does not exist");

            return new Logic(this);
        }

        public override string ToString() => $"DirectoryChangesSource({_directoryPath})";
    }

    public static class DirectoryChangesSource
    {
        /// <summary>
        /// API
        /// </summary>
        /// <param name="directoryPath">Directory to watch</param>
        /// <param name="maxBufferSize">Maximum number of buffered directory changes before the stage fails</param>
        /// <returns></returns>
        public static Source<Tuple<string, WatcherChangeTypes>, NotUsed> Create(string directoryPath, int maxBufferSize)
        {
            var source = new DirectoryChangesSource<Tuple<string, WatcherChangeTypes>>(directoryPath, maxBufferSize, Tuple.Create);
            return Source.FromGraph(source);
        }
    }
}
