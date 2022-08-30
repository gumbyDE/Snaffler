using SnaffCore.Classifiers;
using SnaffCore.Concurrency;
using SnaffCore.Config;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SnaffCore.Config.Options;

namespace SnaffCore.Database
{
    public class DatabaseIndexer
    {
        private BlockingMq Mq { get; set; }
        private BlockingStaticTaskScheduler DatabaseTaskScheduler { get; set; }

        private DatabaseHandler _database;

        public DatabaseIndexer()
        {
            Mq = BlockingMq.GetMq();
            DatabaseTaskScheduler = SnaffCon.GetDatabaseTaskScheduler();

            string dbEngine = MyOptions.DatabaseEngine;

            switch (dbEngine)
            {
                case "MSSQL":
                    _database = new MssqlDatabaseHandler(MyOptions.MaxIndexQueue);
                    break;
                default:
                    throw new ArgumentException("DatabaseEngine");
            }
        }

        /// <summary>
        /// Check if a connection can be made to the database with the configuration given by the user.
        /// If connectivity works, setup the database (create tables and indexes).
        /// </summary>
        /// <returns></returns>
        public bool SetupConnection()
        {
            return _database.SetupConnection();
        }

        /// <summary>
        /// Adds a share for indexing.
        /// </summary>
        /// <param name="share"></param>
        public void AddShare(ShareResult share)
        {
            _database.AddShare(share);
        }


        /// <summary>
        /// Adds a file for indexing.
        /// </summary>
        /// <param name="file"></param>
        public void AddFile(FileInfo file)
        {
            _database.AddFile(file);
        }

        /// <summary>
        /// Saves the content of the internal buffer to the databases.
        /// Blocks until the data has been saved.
        /// </summary>
        public void FlushSync()
        {
            Mq.Degub("Flushing database sync");

            try
            {
                _database.Flush();
            }
            catch (Exception e)
            {
                Mq.Error("Exception in DatabaseIndexer Flush task");
                Mq.Error(e.ToString());
            }
        }

        /// <summary>
        /// Saves the content of the internal buffer to the database.
        /// Uses the DatabaseTaskScheduler to create a new task.
        /// </summary>
        public void Flush()
        {
            Mq.Degub("Flushing database.");

            DatabaseTaskScheduler.New(() =>
            {
                try
                {
                    _database.Flush();
                    // we just flushed the database, but only a specific amount of data is handled by one flush call
                    // if our queue is still full we need to create a new task
                    if (_database.NeedsFlushing()) Flush();
                }
                catch (Exception e)
                {
                    Mq.Error("Exception in DatabaseIndexer Flush task");
                    Mq.Error(e.ToString());
                }
            });
        }
    }
}
