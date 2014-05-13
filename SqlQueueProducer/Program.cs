using System;
using System.Threading;
using Newtonsoft.Json;
using SqlQueue;

namespace SqlQueueProducer
{
    internal class Program
    {
        private static void Main()
        {
            var jobs = new Jobs();
            while (true)
            {
                Thread.Sleep(100);

                var data = new JobData
                {
                    Name = DateTime.Now.ToString("u")
                };
                var job = new Job
                {
                    Id = Guid.NewGuid().ToString("N"),
                    Created = DateTime.Now,
                    Type = data.GetType().AssemblyQualifiedName,
                    Data = JsonConvert.SerializeObject(data),
                };
                jobs.Enqueue(job);

                Console.WriteLine("Created Job '{0}' Name={1}", job.Id, data.Name);
            }
        }
    };
}
