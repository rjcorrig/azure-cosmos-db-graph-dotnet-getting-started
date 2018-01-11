namespace PartitionedGraph
{
    using Gremlin.Net;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;

    class Program
    {
        private static Dictionary<string, string> gremlinQueries = new Dictionary<string, string>
            {
                { "Cleanup",        "g.V().drop()" },
                { "Failed AddVertex 1",    "g.addV('person').property('id', 'thomas').property('age', 44)" }, // Partition key property not specified 
                { "AddVertex 1",    "g.addV('person').property('id', 'thomas').property('firstName', 'Thomas').property('age', 44)" },
                { "AddVertex 2",    "g.addV('person').property('id', 'mary').property('firstName', 'Mary').property('lastName', 'Andersen').property('age', 39)" },
                { "AddVertex 3",    "g.addV('person').property('id', 'ben').property('firstName', 'Ben').property('lastName', 'Miller')" },
                { "AddVertex 4",    "g.addV('person').property('id', 'robin').property('firstName', 'Robin').property('lastName', 'Wakefield')" },
                { "Fetch Node 1",      "g.V('thomas')"}, // fan-out query, no partition key specified
                { "Fetch Node 2",      "g.V('thomas').has('firstName', 'Thomas')" }, // efficient look-up 
                { "Fetch Node 3",      "g.V().has('id','thomas').has('firstName', 'Thomas')" }, // efficient look-up 
                { "Fetch Node 4",      "g.withStrategies(PartitionStrategy.build().partitionKey('firstName').readPartitions('Thomas').create()).V('thomas')"}, // look-up 
                { "AddEdge 1",      "g.V(['Thomas','thomas']).addE('knows').to(g.V(['Mary','mary']))" },
                { "AddEdge 2",      "g.V(['Thomas','thomas']).addE('knows').to(g.V(['Ben','ben']))" },
                { "AddEdge 3",      "g.V(['Ben','ben']).addE('knows').to(g.V(['Robin','robin']))" },
                { "UpdateVertex",   "g.V(['Thomas','thomas']).property('age', 44)" },
                { "CountVertices",  "g.V().count()" }, // fan-out query
                { "Filter Range",   "g.V().hasLabel('person').has('age', gt(40))" }, // fan-out query
                { "Project",        "g.V().hasLabel('person').values('firstName')" }, // fan-out query
                { "Sort",           "g.V().hasLabel('person').order().by('firstName', decr)" }, // fan-out query
                { "Traverse OUT",   "g.V(['Thomas','thomas']).out('knows').hasLabel('person')" },
                { "Traverse IN",    "g.V(['Ben','ben']).in('knows').hasLabel('person')" }, // fan-out query
                { "Traverse 2x",    "g.V('thomas').out('knows').hasLabel('person').out('knows').hasLabel('person')" },
                { "Loop",           "g.V('thomas').repeat(out()).until(has('id', 'robin')).path()" },
                { "DropEdge",       "g.V('thomas').outE('knows').where(inV().has('id', 'mary')).drop()" },
                { "CountEdges",     "g.E().count()" }, // fan-out query
                { "DropVertex",     "g.V(['Thomas','thomas']).drop()" },
                { "DropGraph",      "g.V().drop()" },
            };


        static void Main(string[] args)
        {
            CreatePartitionedGraphAsync().Wait();
            ExecutePartitionedGraphQueriesAsync().Wait();

            Console.WriteLine("Program executed successfully. Press any key to continue.");
            Console.ReadKey();
        }

        private static async Task ExecutePartitionedGraphQueriesAsync()
        {
            try
            {
                GremlinServer server = new GremlinServer(
                    ConfigurationManager.AppSettings["GremlinServerEndPoint"],
                    int.Parse(ConfigurationManager.AppSettings["GremlinServerPort"]),
                    true,
                    "/dbs/" + ConfigurationManager.AppSettings["Database"] + "/colls/" + ConfigurationManager.AppSettings["Collection"],
                    ConfigurationManager.AppSettings["PrimaryKey"]);

                using (GremlinClient gClient = new GremlinClient(server))
                {
                    Console.WriteLine("---------------------------------------------------------------------");
                    foreach (KeyValuePair<string, string> gremlinQuery in Program.gremlinQueries)
                    {
                        Console.WriteLine("Executing: " + gremlinQuery.Key);
                        Console.WriteLine("---------------------------------------------------------------------");
                        await ExecuteGremlinServerQueryAsync(gClient, gremlinQuery.Value);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            
        }

        private static async Task ExecuteGremlinServerQueryAsync(GremlinClient gClient, string query)
        {
            try {

                IEnumerable<dynamic> results =
                             await GremlinClientExtensions.SubmitAsync<dynamic>(gClient, requestScript: query);

                foreach (dynamic result in results)
                {
                    Console.WriteLine(result.ToString());
                }
            }
            catch(Exception ex)
            {
                if (ex.Message.Contains("GraphRuntimeException") && ex.Message.Contains("Add Vertex") && ex.Message.Contains("Partition key property must be provided"))
                {
                    Console.WriteLine("Add Vertex failed, as partition key property was not specified while adding a vertex to a partitioned graph.");
                }
                else
                {
                    throw;
                }
            }

            Console.WriteLine("---------------------------------------------------------------------");
        }

        private static async Task CreatePartitionedGraphAsync()
        {
            DocumentClient client = new DocumentClient(
               new Uri(ConfigurationManager.AppSettings["DocumentServerEndPoint"]),
               ConfigurationManager.AppSettings["PrimaryKey"],
               new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp });

            string databaseId = ConfigurationManager.AppSettings["Database"];
            string collectionId = ConfigurationManager.AppSettings["Collection"];
            string partitionKey = ConfigurationManager.AppSettings["PartitionKeyName"];


            Database database =
                client.CreateDatabaseQuery()
                    .Where(db => db.Id == databaseId)
                    .AsEnumerable()
                    .FirstOrDefault();

            DocumentCollection collection = null;

            try
            {
                collection = await client.ReadDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(databaseId, collectionId)).ConfigureAwait(false);

                if(collection != null)
                {
                    if(collection.PartitionKey != null && collection.PartitionKey.Paths != null && collection.PartitionKey.Paths.Count < 1)
                    {
                        Console.WriteLine(string.Format("Graph collection with name {0} already exists. However it's not a partioned graph.", collectionId));
                        Console.WriteLine("Application will exit now. Press any key to continue."); Console.ReadKey();
                        throw new InvalidOperationException("An incompatible graph collection already exists.\n");
                    }
                    else
                    {
                        if(!collection.PartitionKey.Paths[0].Equals("/"+partitionKey))
                        {
                            Console.WriteLine(string.Format("A partitioned Graph collection with name {0} already exists.", collectionId));
                            Console.WriteLine(string.Format("However it's partion key: {0} doesn't match with the one specified in App.config.", collection.PartitionKey.Paths[0]));
                            Console.WriteLine("Application will exit now. Press any key to continue."); Console.ReadKey();
                            throw new InvalidOperationException("An incompatible graph collection already exists.\n");
                        }
                        else
                        {
                            Console.WriteLine(string.Format("A graph collection named: {0} with macthing configuration (App.config) already exists", collectionId));
                            Console.WriteLine("Press any key to continue executing queries.\n"); Console.ReadKey();
                        }
                    }
                }
            }
            catch (DocumentClientException exception) when (exception.StatusCode == HttpStatusCode.NotFound)
            {
                int throughput = int.Parse(ConfigurationManager.AppSettings["Throughput"]);
                bool isPartitionedGraph = bool.Parse(ConfigurationManager.AppSettings["IsPartitionedGraph"]);

                Console.WriteLine(string.Format("No graph found. Creating a graph collection: {0} with throughput = {1}", collectionId, throughput));
                if (isPartitionedGraph)
                {
                    Console.WriteLine(string.Format("The collection is a partitioned collection with partition Key: /{0}", partitionKey));
                }
                else
                {
                    Console.WriteLine($"The collection is a fixed collection with no partition Key");
                }
                Console.WriteLine("Press any key to continue ...");
                Console.ReadKey();

                DocumentCollection myCollection = new DocumentCollection
                {
                    Id = collectionId
                };

                if (isPartitionedGraph)
                {
                    if (string.IsNullOrWhiteSpace(partitionKey))
                    {
                        throw new ArgumentNullException("PartionKey can't be null for a partitioned collection");
                    }

                    myCollection.PartitionKey.Paths.Add("/" + partitionKey);
                }

                collection = await client.CreateDocumentCollectionAsync(
                    database.SelfLink,
                    myCollection,
                    new RequestOptions { OfferThroughput = throughput });
            }
        }
    }
}
