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
        private const string _sqlCreateShareTable = @"
            CREATE TABLE shares (
                pk_column data_type PRIMARY KEY,
                column_1 data_type NOT NULL,
                column_2 data_type,
                ...,
                table_constraints
            );
        ";

        private const string _sqlInsertShare = @"

        ";

        private const string _sqlInsertFile = @"

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
            }

            if (!string.IsNullOrWhiteSpace(MyOptions.DatabaseUsername))
            {
                connectionStringBuilder.Append("User Id=");
                connectionStringBuilder.Append(MyOptions.DatabaseUsername);
            }

            if (!string.IsNullOrWhiteSpace(MyOptions.DatabaseUsername))
            {
                connectionStringBuilder.Append("Password=");
                connectionStringBuilder.Append(MyOptions.DatabasePassword);
            }

            if (!string.IsNullOrWhiteSpace(MyOptions.DatabaseSchema))
            {
                connectionStringBuilder.Append("Database=");
                connectionStringBuilder.Append(MyOptions.DatabaseSchema);
            }

            _connectionString = connectionStringBuilder.ToString();
        }

        public override bool CheckConnection()
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                }
                return true;
            }
            catch (Exception e)
            {
                Mq.Error("Could not connect to database");
                Mq.Error(e.ToString());
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

                try
                {
                    while (_fileBuffer.TryTake(out currentFile))
                    {

                        command.CommandText = _sqlInsertFile;

                        command.Parameters.Add(new SqlParameter("@fullname", currentFile.FullName));
                        command.Parameters.Add(new SqlParameter("@filename", currentFile.Name));
                        command.Parameters.Add(new SqlParameter("@size", currentFile.Length));
                        command.Parameters.Add(new SqlParameter("@extension", currentFile.Extension));

                        command.Prepare();
                        command.ExecuteNonQuery();
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

                try
                {
                    while (_shareBuffer.TryTake(out currentShare))
                    {

                        command.CommandText = _sqlInsertFile;

                        command.Parameters.Add(new SqlParameter("@computer", currentShare.Computer));
                        command.Parameters.Add(new SqlParameter("@path", currentShare.SharePath));
                        command.Parameters.Add(new SqlParameter("@comment", currentShare.ShareComment));

                        command.Prepare();
                        command.ExecuteNonQuery();
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