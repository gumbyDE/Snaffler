using SnaffCore.Classifiers;
using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using static SnaffCore.Config.Options;

namespace SnaffCore.Database
{
    public class MssqlDatabaseHandler : DatabaseHandler
    {
        private const string _sqlCreateSharesTable = @"
            IF OBJECT_ID(N'dbo.shares', N'U') IS NULL
            BEGIN
                CREATE TABLE shares (
	                computer varchar(250) NOT NULL,
	                sharename varchar(100) NOT NULL,
	                comment varchar(1000) NULL
                );
            END;
        ";

        /*
         * Don't create indexes right away, this kills the performance of the mssql server
                CREATE UNIQUE NONCLUSTERED INDEX ix_files_fullname ON files (fullname ASC)
                WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = ON, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
                CREATE NONCLUSTERED INDEX ix_files_filename ON files (filename ASC)
                WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
        */
        private const string _sqlCreateFilesTable = @"
            IF OBJECT_ID(N'dbo.files', N'U') IS NULL
            BEGIN
                CREATE TABLE files (
	                fullname varchar(1000) NOT NULL,
	                filename varchar(500) NOT NULL,
	                size bigint NULL,
	                extension varchar(100) NULL
                );

            END;
        ";

        private const string _sqlInsertShare = @"
            INSERT INTO shares (computer, sharename, comment)
            VALUES (@computer, @sharename, @comment);
        ";

        private const string _sqlInsertFile = @"
            INSERT INTO files (fullname, filename, size, extension)
            VALUES (@fullname, @filename, @size, @extension);
        ";

        private readonly string _connectionString;

        public MssqlDatabaseHandler(int maxBufferSize) : base(maxBufferSize)
        {
            StringBuilder connectionStringBuilder = new StringBuilder();

            if (!string.IsNullOrWhiteSpace(MyOptions.DatabaseHost))
            {
                connectionStringBuilder.Append("Server=");
                connectionStringBuilder.Append(MyOptions.DatabaseHost);

                if (MyOptions.DatabasePort > 0)
                {
                    connectionStringBuilder.Append(",");
                    connectionStringBuilder.Append(MyOptions.DatabasePort);
                }
                connectionStringBuilder.Append(';');
            }

            if (!string.IsNullOrWhiteSpace(MyOptions.DatabaseUsername))
            {
                connectionStringBuilder.Append("User Id=");
                connectionStringBuilder.Append(MyOptions.DatabaseUsername);
                connectionStringBuilder.Append(';');
            }

            if (!string.IsNullOrWhiteSpace(MyOptions.DatabaseUsername))
            {
                connectionStringBuilder.Append("Password=");
                connectionStringBuilder.Append(MyOptions.DatabasePassword);
                connectionStringBuilder.Append(';');
            }

            if (!string.IsNullOrWhiteSpace(MyOptions.DatabaseSchema))
            {
                connectionStringBuilder.Append("Database=");
                connectionStringBuilder.Append(MyOptions.DatabaseSchema);
                connectionStringBuilder.Append(';');
            }

            _connectionString = connectionStringBuilder.ToString();
        }

        public override bool SetupConnection()
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    SqlCommand commandShares = connection.CreateCommand();
                    SqlCommand commandFiles = connection.CreateCommand();
                    commandShares.CommandText = _sqlCreateSharesTable;
                    commandFiles.CommandText = _sqlCreateFilesTable;
                    commandShares.ExecuteNonQuery();
                    commandFiles.ExecuteNonQuery();
                }
                return true;
            }
            catch (Exception e)
            {
                Mq.Error("Got an error while trying to setup database: " + e.Message);
                Mq.Degub(e.ToString());
                return false;
            }
        }

        protected override void InsertFiles()
        {
            FileInfo currentFile = null;

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                SqlCommand command = connection.CreateCommand();
                SqlTransaction transaction = connection.BeginTransaction();

                command.Connection = connection;
                command.Transaction = transaction;
                command.CommandText = _sqlInsertFile;

                try
                {
                    while (_fileBuffer.TryTake(out currentFile))
                    {
                        command.Parameters.AddWithValue("@fullname", currentFile.FullName);
                        command.Parameters.AddWithValue("@filename", currentFile.Name);
                        command.Parameters.AddWithValue("@size", currentFile.Length);
                        command.Parameters.AddWithValue("@extension", currentFile.Extension);

                        command.ExecuteNonQuery();
                        command.Parameters.Clear();
                    }
                    transaction.Commit();
                    Mq.Degub("Commited transaction");
                }
                catch (Exception ex1)
                {
                    Mq.Error("Could not insert files into database, have to rollback transaction");
                    Mq.Error(ex1.ToString());

                    try
                    {
                        transaction.Rollback();
                    }
                    catch (Exception ex2)
                    {
                        Mq.Error("Could not rollback transaction");
                        Mq.Error(ex2.ToString());
                    }
                }
            }
        }

        protected override void InsertShares()
        {
            ShareResult currentShare = null;

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                SqlCommand command = connection.CreateCommand();
                SqlTransaction transaction = connection.BeginTransaction();

                command.Connection = connection;
                command.Transaction = transaction;
                command.CommandText = _sqlInsertShare;

                try
                {
                    while (_shareBuffer.TryTake(out currentShare))
                    {
                        string shareName = currentShare.SharePath
                            .TrimStart('\\')
                            .Substring(currentShare.Computer.Length + 1);
                        command.Parameters.AddWithValue("@computer", currentShare.Computer);
                        command.Parameters.AddWithValue("@sharename", shareName);
                        command.Parameters.AddWithValue("@comment", currentShare.ShareComment);

                        command.ExecuteNonQuery();
                        command.Parameters.Clear();
                    }
                    transaction.Commit();
                    Mq.Degub("Commited transaction");
                }
                catch (Exception ex1)
                {
                    Mq.Error("Could not insert shares into database, have to rollback transaction");
                    Mq.Error(ex1.ToString());

                    try
                    {
                        transaction.Rollback();
                    }
                    catch (Exception ex2)
                    {
                        Mq.Error("Could not rollback transaction");
                        Mq.Error(ex2.ToString());
                    }
                }
            }
        }
    }
}