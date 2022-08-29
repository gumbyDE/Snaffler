using SnaffCore.Classifiers;
using SnaffCore.Concurrency;
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

        public void AddShare(ShareResult share)
        {
            _database.AddShare(share);
        }

        public void AddFile(FileInfo file)
        {
            _database.AddFile(file);
        }

        public void Flush()
        {
            Mq.Degub("Flushing database.");

            DatabaseTaskScheduler.New(() =>
            {
                try
                {
                    _database.Flush();
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
