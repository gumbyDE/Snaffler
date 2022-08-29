﻿using SnaffCore.Classifiers;
using SnaffCore.Concurrency;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SnaffCore.Database
{
    public abstract class DatabaseHandler
    {
        protected BlockingMq Mq { get; set; }

        protected ConcurrentBag<ShareResult> _shareBuffer = new ConcurrentBag<ShareResult>();
        protected ConcurrentBag<FileInfo> _fileBuffer = new ConcurrentBag<FileInfo>();

        private readonly int _maxBufferSize;

        public DatabaseHandler(int maxBufferSize)
        {
            Mq = BlockingMq.GetMq();
            _maxBufferSize = maxBufferSize;
        }

        public void AddShare(ShareResult share)
        {
            _shareBuffer.Add(share);
            if (_shareBuffer.Count + _fileBuffer.Count >= _maxBufferSize)
            {
                Flush();
            }
        }

        public void AddFile(FileInfo file)
        {
            _fileBuffer.Add(file);
            if (_shareBuffer.Count + _fileBuffer.Count >= _maxBufferSize)
            {
                Flush();
            }
        }

        /// <summary>
        /// Force the buffered data to be written to the database.
        /// This function is automatically executed if the current buffer exceeds the buffer size set by MaxIndexQueue
        /// </summary>
        public void Flush()
        {
            InsertShares();
            InsertFiles();
        }

        public abstract bool CheckConnection();

        protected abstract void InsertShares();
        protected abstract void InsertFiles();
    }
}
