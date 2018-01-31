namespace GraphGetStarted
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Runtime.Versioning;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Configuration;
    using Newtonsoft.Json;
    using Gremlin.Net.Driver;
    using Gremlin.Net.Structure.IO.GraphSON;
    using Newtonsoft.Json.Linq;

    internal class RelationIdentifier
    {
        public static string GraphsonPrefix = "janusgraph";
        public static string GraphsonBaseType = "RelationIdentifier";
        public static string GraphsonType = GraphSONUtil.FormatTypeName(GraphsonPrefix, GraphsonBaseType);

        public RelationIdentifier(string value)
        {
            Value = value;
        }

        public string Value { get; }
    }

    internal class RelationIdentifierReader : IGraphSONDeserializer
    {
        public dynamic Objectify(JToken graphsonObject, GraphSONReader reader)
        {
            var value = reader.ToObject(graphsonObject["value"]);
            return new RelationIdentifier(value);
        }
    }

    /// <summary>
    /// Sample program that shows how to get started with the Graph (Gremlin) APIs for Azure Cosmos DB.
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Runs some Gremlin commands on the console.
        /// </summary>
        /// <param name="args">command-line arguments</param>
        public static void Main(string[] args)
        {
            IConfigurationRoot builder = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();

            string server = builder["gremlinConnection:server"];
            if (!short.TryParse(builder["gremlinConnection:port"], out short port))
            {
                port = 8182;
            }

            Console.WriteLine($"Running GraphGetStarted {((TargetFrameworkAttribute)Assembly.GetExecutingAssembly().GetCustomAttributes(typeof(TargetFrameworkAttribute), false)[0]).FrameworkName }");

            var graphSONReader = new GraphSON2Reader(new Dictionary<string, IGraphSONDeserializer>
            {
                {
                    RelationIdentifier.GraphsonType, new RelationIdentifierReader()
                }
            });

            using (GremlinClient client = new GremlinClient(
                new GremlinServer(server, port),
                mimeType: GremlinClient.GraphSON2MimeType,
                graphSONReader: graphSONReader))
            {
                Program p = new Program();

                p.RunAsync(client).Wait();
            }
        }

        /// <summary>
        /// Run the get started application.
        /// </summary>
        /// <param name="client">The Gremlin client instance</param>
        /// <returns>A Task for asynchronous execuion.</returns>
        public async Task RunAsync(GremlinClient client)
        {
            // JanusGraph supports the Gremlin API for working with Graphs. Gremlin is a functional programming language composed of steps.
            // Here, we run a series of Gremlin queries to show how you can add vertices, edges, modify properties, perform queries and traversals
            // For additional details, see https://aka.ms/gremlin for the complete list of supported Gremlin operators
            Dictionary<string, string> gremlinQueries = new Dictionary<string, string>
            {
                { "Cleanup",        "g.V().drop()" },
                { "AddVertex 1",    "g.addV('person').property('id', 'thomas').property('firstName', 'Thomas').property('age', 44)" },
                { "AddVertex 2",    "g.addV('person').property('id', 'mary').property('firstName', 'Mary').property('lastName', 'Andersen').property('age', 39)" },
                { "AddVertex 3",    "g.addV('person').property('id', 'ben').property('firstName', 'Ben').property('lastName', 'Miller')" },
                { "AddVertex 4",    "g.addV('person').property('id', 'robin').property('firstName', 'Robin').property('lastName', 'Wakefield')" },
                { "AddEdge 1",      "g.V().has('id','thomas').addE('knows').to(g.V().has('id','mary'))" },
                { "AddEdge 2",      "g.V().has('id','thomas').addE('knows').to(g.V().has('id','ben'))" },
                { "AddEdge 3",      "g.V().has('id','ben').addE('knows').to(g.V().has('id','robin'))" },
                { "UpdateVertex",   "g.V().has('id','thomas').property('age', 44)" },
                { "CountVertices",  "g.V().count()" },
                { "Filter Range",   "g.V().hasLabel('person').has('age', gt(40))" },
                { "Project",        "g.V().hasLabel('person').values('firstName')" },
                { "Sort",           "g.V().hasLabel('person').order().by('firstName', decr)" },
                { "Traverse",       "g.V().has('id','thomas').out('knows').hasLabel('person')" },
                { "Traverse 2x",    "g.V().has('id','thomas').out('knows').hasLabel('person').out('knows').hasLabel('person')" },
                { "Loop",           "g.V().has('id','thomas').repeat(out()).until(has('id', 'robin')).path()" },
                { "DropEdge",       "g.V().has('id','thomas').outE('knows').where(inV().has('id', 'mary')).drop()" },
                { "CountEdges",     "g.E().count()" },
                { "DropVertex",     "g.V().has('id','thomas').drop()" },
            };

            foreach (KeyValuePair<string, string> gremlinQuery in gremlinQueries)
            {
                Console.WriteLine($"Running {gremlinQuery.Key}: {gremlinQuery.Value}");

                var query = await client.SubmitAsync<object>(gremlinQuery.Value);
                foreach (var result in query)
                {
                    Console.WriteLine($"\t {JsonConvert.SerializeObject(result)}");
                }

                Console.WriteLine();
            }

            string gremlin = gremlinQueries["AddVertex 1"] + ".valueMap()";
            Console.WriteLine($"Running Add Vertex with valueMap: {gremlin}");
            var valueMaps = await client.SubmitAsync<dynamic>(gremlin);
            foreach (var valueMap in valueMaps)
            {
                // Gremlin is designed for multi-valued properties, each value is an array
                Console.WriteLine($"\t {JsonConvert.SerializeObject(valueMap)}");
            }

            Console.WriteLine();

            Console.WriteLine("Done. Press any key to exit...");
            Console.ReadLine();
        }
    }
}
