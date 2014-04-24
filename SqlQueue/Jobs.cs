using System;
using System.IO;
using System.Threading;

namespace SqlQueue
{
    public class Job
    {
        public string Id { get; set; }
        public DateTime Created { get; set; }
        public DateTime? Processed { get; set; }
        public DateTime? Completed { get; set; }
    };

    public class Jobs
    {
        private readonly string _dbFile;
        private readonly string _connectionString;

        public Jobs()
        {
            _dbFile = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), @"..\..\..\Data\Queue.mdf"));
            var cs = @"Server=.\SQLEXPRESS;AttachDBFilename=" + _dbFile + ";User Instance=true;Integrated Security=SSPI;MultipleActiveResultSets=True;";
            _connectionString = cs;
            CheckForTable();
        }
        
        public bool UseDequeue { get; set; }

        private void CheckForTable()
        {
            if (!File.Exists(_dbFile))
            {
                using (var db = new PetaPoco.Database(@"Server=.\SQLEXPRESS;Database=master;Integrated Security=SSPI;", "System.Data.SqlClient"))
                {
                    db.Execute(string.Format(@"CREATE DATABASE [Queue] ON PRIMARY (NAME = Queue_Data, FILENAME = '{0}')", _dbFile));
                    db.Execute("EXEC sp_detach_db 'Queue', 'true'");
                }
            }

            using (var db = GetDb())
            {
                db.Execute(@"
                    IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='Jobs')) BEGIN
                        CREATE TABLE [Jobs] (Id varchar(50) PRIMARY KEY, Created datetime, Processed datetime NULL, Completed datetime NULL);
                    END
                ");
            }
        }

        public void Enqueue(Job job)
        {
            using (var db = GetDb())
            {
                db.Insert("Jobs", "Id", false, job);
            }
        }

        public Job Dequeue()
        {
            Job job;
            using (var db = GetDb())
            {
                job = UseDequeue
                          ? db.FirstOrDefault<Job>(DEQUEUE.Replace("@", "@@"))
                          : db.FirstOrDefault<Job>("SELECT TOP 1 * FROM Jobs WHERE Processed IS NULL");

                if (job == null)
                    return null;
            }

            if (!UseDequeue)
            {
                if (job.Processed.HasValue)
                    throw new ApplicationException("Job already processing: " + job.Id);

                job.Processed = DateTime.Now;
                using (var db = GetDb())
                {
                    db.Save("Jobs", "Id", job);
                }
            }

            return job;
        }

        public void Complete(string id)
        {
            using (var db = GetDb())
            {
                var job = db.Single<Job>("SELECT TOP 1 * FROM Jobs WHERE Id=@0", id);

                if (job.Completed.HasValue)
                    throw new ApplicationException("Job already completed: " + job.Id);

                job.Completed = DateTime.Now;
                db.Save("Jobs", "Id", job);
            }
        }

        private PetaPoco.Database GetDb()
        {
            var db = new PetaPoco.Database(_connectionString, "System.Data.SqlClient");
            db.EnableAutoSelect = false;
            return db;
        }

        private static string DEQUEUE = @"
            SET NOCOUNT ON
            DECLARE @BatchSize INT = 1
            DECLARE @Batch TABLE (Id varchar(MAX))
            DECLARE @Date DATETIME = GETDATE()
            DECLARE @Seconds INT = 300
            BEGIN TRAN

            INSERT INTO @Batch
            SELECT TOP (@BatchSize) Id
            FROM Jobs WITH (UPDLOCK, HOLDLOCK)
            WHERE Completed IS NULL AND (Processed IS NULL OR DATEDIFF(SECOND, Processed, @Date)>@Seconds)
            ORDER BY Created ASC

            DECLARE @ItemsToUpdate INT = @@ROWCOUNT

            UPDATE Jobs
            SET Processed = GETDATE()
            WHERE Id IN (SELECT Id FROM @Batch)
            AND Completed IS NULL AND (Processed IS NULL OR DATEDIFF(SECOND, Processed, @Date)>@Seconds)

            IF @@ROWCOUNT = @ItemsToUpdate BEGIN
                COMMIT TRAN
                SELECT j.*
                FROM @Batch b
                INNER JOIN Jobs j ON j.Id = b.Id
                PRINT 'SUCCESS'
            END ELSE BEGIN
                ROLLBACK TRAN
                PRINT 'FAILED'
            END
        ";
    };

    public static class Producer
    {
        public static void Run()
        {
            var jobs = new Jobs();
            while (true)
            {
                Thread.Sleep(100);

                var job = new Job
                          {
                              Id = Guid.NewGuid().ToString("N"),
                              Created = DateTime.Now,
                          };
                jobs.Enqueue(job);

                Console.WriteLine("Created Job '{0}' at '{1:hh:mm:ss.fff}'", job.Id, job.Created);
            }
        }
    };

    public static class Consumer
    {
        public static void Run()
        {
            var random = new Random();
            var jobs = new Jobs { UseDequeue = true }; //Use false to test without proc
            var quit = false;
            var waiting = false;

            while (!quit)
            {
                var job = jobs.Dequeue();

                if (job == null)
                {
                    if (!waiting)
                    {
                        waiting = true;
                        Console.WriteLine("Waiting for job...");
                    }
                    continue;
                }

                waiting = false;
                Thread.Sleep(100 * random.Next(1, 20)); //do random work

                try
                {
                    jobs.Complete(job.Id);
                    Console.WriteLine("Completed job '{0}'", job.Id);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("> ERROR: " + ex.Message);
                    quit = true;
                    Console.ReadKey();
                }
            }
        }
    };
}
