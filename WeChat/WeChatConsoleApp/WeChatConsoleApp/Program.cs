using System;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceBus.Description;
using System.Data;
using System.IO;

namespace Microsoft.ServiceBus.Samples
{
    class Program
    {

        private static DataTable issues;
        private static List<BrokeredMessage> MessageList;

        private static string ServiceNamespace;
        private static string IssuerName;
        private static string IssuerKey;


        public static void Main(string[] args)
        {

            // Populate test data
            issues = ParseCSVFile();

            MessageList = GenerateMessages(issues);

            CollectUserInput();

            Queue();


        }
        static DataTable ParseCSVFile()
        {
            DataTable tableIssues = new DataTable("Issues");
            string path = @"..\..\data.csv";
            try
            {
                using (StreamReader readFile = new StreamReader(path))
                {
                    string line;
                    string[] row;

                    // create the columns
                    line = readFile.ReadLine();
                    foreach (string columnTitle in line.Split(','))
                    {
                        tableIssues.Columns.Add(columnTitle);
                    }

                    while ((line = readFile.ReadLine()) != null)
                    {
                        row = line.Split(',');
                        tableIssues.Rows.Add(row);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error:" + e.ToString());
            }

            return tableIssues;
        }

        static List<BrokeredMessage> GenerateMessages(DataTable issues)
        {
            // Instantiate the brokered list object
            List<BrokeredMessage> result = new List<BrokeredMessage>();

            // Iterate through the table and create a brokered message for each row
            foreach (DataRow item in issues.Rows)
            {
                BrokeredMessage message = new BrokeredMessage();
                foreach (DataColumn property in issues.Columns)
                {
                    message.Properties.Add(property.ColumnName, item[property]);
                }
                result.Add(message);
            }
            return result;
        }

        static void CollectUserInput()
        {
            // User service namespace
            Console.Write("Please enter the service namespace to use: ");
            ServiceNamespace = Console.ReadLine();

            // Issuer name
            Console.Write("Please enter the issuer name to use: ");
            IssuerName = Console.ReadLine();

            // Issuer key
            Console.Write("Please enter the issuer key to use: ");
            IssuerKey = Console.ReadLine();
        }


        static void Queue()
        {
            // Create management credentials
            TokenProvider credentials =
                        TokenProvider.CreateSharedSecretTokenProvider(IssuerName, IssuerKey);

            NamespaceManager namespaceClient = new NamespaceManager(ServiceBusEnvironment.CreateServiceUri("sb", ServiceNamespace, string.Empty), credentials);

            //QueueDescription myQueue;
            //myQueue = namespaceClient.CreateQueue("IssueTrackingQueue");
            namespaceClient.CreateQueue("IssueTrackingQueue");

            MessagingFactory factory = MessagingFactory.Create(ServiceBusEnvironment.CreateServiceUri("sb", ServiceNamespace, string.Empty), credentials);

            QueueClient myQueueClient = factory.CreateQueueClient("IssueTrackingQueue");

            // Create a sender
            //MessageSender myMessageSender = myQueueClient.CreateSender();

            // Send messages
            Console.WriteLine("Now sending messages to the Queue.");
            for (int count = 0; count < 6; count++)
            {
                var issue = MessageList[count];
                issue.Label = issue.Properties["IssueTitle"].ToString();
                myQueueClient.Send(issue);
                Console.WriteLine(string.Format("Message sent: {0}, {1}", issue.Label, issue.MessageId));
            }

            Console.WriteLine("Now receiving messages from Queue.");
            BrokeredMessage message;
            while ((message = myQueueClient.Receive(new TimeSpan(hours: 0, minutes: 1, seconds: 5))) != null)
            {
                Console.WriteLine(string.Format("Message received: {0}, {1}, {2}", message.SequenceNumber, message.Label, message.MessageId));
                message.Complete();

                Console.WriteLine("Processing message (sleeping...)");
                Thread.Sleep(1000);
            }

            factory.Close();
            myQueueClient.Close();
            namespaceClient.DeleteQueue("IssueTrackingQueue");
        }

    }

}