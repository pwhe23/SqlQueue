using System;
using System.Threading;
using Newtonsoft.Json;
using SqlQueue;

namespace SqlQueueConsumer
{
    class Program
    {
        static void Main()
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
                var data = (JobData) JsonConvert.DeserializeObject(job.Data, Type.GetType(job.Type));

                try
                {
                    jobs.Complete(job.Id);
                    Console.WriteLine("Completed job '{0}' Name={1}", job.Id, data.Name);
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
