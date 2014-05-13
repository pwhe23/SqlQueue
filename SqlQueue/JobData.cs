
namespace SqlQueue
{
    /// <summary>
    /// Example class where you can store job-specific data. The Type of this class can be stored in Job.Type and the serialized instance in Job.Data.
    /// </summary>
    public class JobData
    {
        public string Name { get; set; }
    };
}
