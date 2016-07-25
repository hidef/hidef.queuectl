using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System.IO;
using System.Threading.Tasks;

namespace HiDef.QueueCtl
{
    public class InputException : Exception
    {
        public InputException(string message): base(message) { }
    }
    
    public static class CLIExtensions
    {
        public static string Require(this string[] self, int index, string errorMessage)
        {
            if ( self.Length <= index ) throw new InputException(errorMessage);
            else return self[index];
        }
    }
    
    public class Program
    {
        public void Main(string[] args)
        {
            //  foreach (string arg in args)
            //  {
            //      Console.WriteLine(arg);
            //  }
            try 
            {
                args.Require(0, @"Usage: 
    hidef.queuectl enqueue <inputPath> <outputConnectionString> <outputQueue>
    hidef.queuectl dequeue <inputConnectionString> <inputQueue> <outputPath>");
        
                switch ( args[0].ToLowerInvariant() )
                {
                    case "enqueue": 
                        enqueue(args.Skip(1).ToArray());
                        break;
                    case "dequeue": 
                        dequeue(args.Skip(1).ToArray());
                        break;
                }
            }
            catch ( InputException inputException)
            {
                Console.WriteLine(inputException.Message);
            }
        }
        
        private void enqueue(string[] args)
        {
            Console.WriteLine(args[0]);
            Console.WriteLine(args[1]);
            Console.WriteLine(args[2]);
            string inputPath = args.Require(0, "Path to read messages from is mandatory.");
            string outputConnectionString = args.Require(1, "Output connection string is mandatory.");
            string outputQueue = args.Require(2, "Output queue is mandatory");
            Console.WriteLine("connection string: " + outputConnectionString);
            IReader reader = new DiskReader(inputPath);
            IWriter writer = new QueueWriter(outputConnectionString, outputQueue);
            Console.WriteLine("beginning");
            int i = 0;
            foreach(var message in reader.Read())
            {
                writer.Write(message).Wait();
                Console.Write(i++);
            }
            Console.WriteLine("complete");
        }
        
        private void dequeue(string[] args)
        {
            string inputConnectionString = args.Require(0, "Azure connection string from which to receive messages is mandatory.");
            string inputQueue = args.Require(1, "The queue name from which to receive messages is mandatory.");
            
            string outputPath = args.Require(2, "An output path to write messages to is mandatory.");
            
            IReader reader = new QueueReader(inputConnectionString, inputQueue);
            IWriter writer = new DiskWriter(outputPath);
            
            foreach(var message in reader.Read())
            {
                writer.Write(message).Wait();
            }
        }
    }
    
    public interface IReader
    {
        IEnumerable<string> Read();
    }
    
    public interface IWriter
    {
        Task Write(string content);
    }

    public abstract class QueueClient
    {
        protected readonly CloudQueue Queue;
        
        private readonly CloudStorageAccount _account;
        public QueueClient(string connectionString, string queue)
        {
            _account = CloudStorageAccount.Parse(connectionString);
            var queueClient = _account.CreateCloudQueueClient();
            this.Queue = queueClient.GetQueueReference(queue);
            this.Queue.CreateIfNotExistsAsync().Wait();
        }
    }

    public class QueueReader : QueueClient, IReader
    {
        public QueueReader(string a, string b) : base(a, b) { }

        public IEnumerable<string> Read()
        {
            bool noMessagesReceived = false;
            while ( !noMessagesReceived )
            {
                noMessagesReceived = true;
                foreach ( var message in base.Queue.GetMessagesAsync(1000).Result)
                {
                    noMessagesReceived = false;
                    yield return message.AsString;
                    //base.Queue.DeleteMessageAsync(message);
                }
            }
        }
    }

    public class QueueWriter : QueueClient, IWriter
    {
        public QueueWriter(string a, string b) : base(a, b) { }

        public async Task Write(string content)
        {
            await base.Queue.AddMessageAsync(new CloudQueueMessage(content));
            Console.Write("W");
        }
    }


    public class DiskReader : IReader
    {
        private readonly string _path;
        
        public DiskReader(string path)
        {
            _path = path;   
        }
        
        public IEnumerable<string> Read()
        {
            foreach ( FileInfo fi in new DirectoryInfo(_path).EnumerateFiles("*.txt").ToArray())
            {
                using ( var readStream = fi.OpenRead())
                using ( var streamReader = new StreamReader(readStream))
                {
                    Console.Write("r");
                    yield return streamReader.ReadToEnd();
                }
                
                fi.MoveTo(Path.ChangeExtension(fi.FullName, "done"));
            }
        }
    }

    public class DiskWriter : IWriter
    {
        private readonly string _path;
        
        public DiskWriter(string path)
        {
            if (path == null)
                throw new ArgumentNullException("path");
                
            _path = path;
        }
        public Task Write(string content)
        {
            File.WriteAllText(Path.Combine(_path, Guid.NewGuid().ToString() + ".dat"), content);
            return Task.FromResult(0);
        }
    }
}
