using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDB2
{
    class AwsSample
    {
        private AmazonDynamoDBClient _client;

        public async Task Run()
        {
            var clientConfig = new AmazonDynamoDBConfig { ServiceURL = "http://localhost:8000" };
            _client = new AmazonDynamoDBClient(clientConfig);

            try
            {
                // DeleteAllTables(client);
                await TableTools.DeleteTable(_client, "ProductCatalog");
                await TableTools.DeleteTable(_client, "Forum");
                await TableTools.DeleteTable(_client, "Thread");
                await TableTools.DeleteTable(_client, "Reply");

                // Create tables (using the AWS SDK for .NET low-level API).
                await CreateTableProductCatalog();
                await CreateTableForum();
                await CreateTableThread(); // ForumTitle, Subject */
                await CreateTableReply();

                // Load data (using the .NET SDK document API)
                await LoadSampleProducts();
                await LoadSampleForums();
                await LoadSampleThreads();
                await LoadSampleReplies();

                Console.WriteLine("Sample complete!");
                Console.WriteLine("Press ENTER to continue");
                Console.ReadLine();
            }
            catch (AmazonServiceException e) { Console.WriteLine(e.Message); }
            catch (Exception e) { Console.WriteLine(e.Message); }
        }

        private async Task CreateTableProductCatalog()
        {
            string tableName = "ProductCatalog";

            var response = await _client.CreateTableAsync(new CreateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Id",
                        AttributeType = "N"
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Id",
                        KeyType = "HASH"
                    }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 10,
                    WriteCapacityUnits = 5
                }
            });

            await TableTools.WaitTillTableCreated(_client, tableName, response);
        }

        private async Task CreateTableForum()
        {
            string tableName = "Forum";

            var response = await _client.CreateTableAsync(new CreateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Name",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Name", // forum Title
                        KeyType = "HASH"
                    }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 10,
                    WriteCapacityUnits = 5
                }
            });

            await TableTools.WaitTillTableCreated(_client, tableName, response);
        }

        private async Task CreateTableThread()
        {
            string tableName = "Thread";

            var response = await _client.CreateTableAsync(new CreateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "ForumName", // Hash attribute
                        AttributeType = "S"
                    },
                    new AttributeDefinition
                    {
                        AttributeName = "Subject",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "ForumName", // Hash attribute
                        KeyType = "HASH"
                    },
                    new KeySchemaElement
                    {
                        AttributeName = "Subject", // Range attribute
                        KeyType = "RANGE"
                    }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 10,
                    WriteCapacityUnits = 5
                }
            });

            await TableTools.WaitTillTableCreated(_client, tableName, response);
        }

        private async Task CreateTableReply()
        {
            string tableName = "Reply";
            var response = await _client.CreateTableAsync(new CreateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Id",
                        AttributeType = "S"
                    },
                    new AttributeDefinition
                    {
                        AttributeName = "ReplyDateTime",
                        AttributeType = "S"
                    },
                    new AttributeDefinition
                    {
                        AttributeName = "PostedBy",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement()
                    {
                        AttributeName = "Id",
                        KeyType = "HASH"
                    },
                    new KeySchemaElement()
                    {
                        AttributeName = "ReplyDateTime",
                        KeyType = "RANGE"
                    }
                },
                LocalSecondaryIndexes = new List<LocalSecondaryIndex>()
                {
                    new LocalSecondaryIndex()
                    {
                        IndexName = "PostedBy_index",


                        KeySchema = new List<KeySchemaElement>() {
                            new KeySchemaElement() {
                                AttributeName = "Id", KeyType = "HASH"
                            },
                            new KeySchemaElement() {
                                AttributeName = "PostedBy", KeyType = "RANGE"
                            }
                        },
                        Projection = new Projection() {
                            ProjectionType = ProjectionType.KEYS_ONLY
                        }
                    }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 10,
                    WriteCapacityUnits = 5
                }
            });

            await TableTools.WaitTillTableCreated(_client, tableName, response);
        }

        private async Task LoadSampleProducts()
        {
            var productCatalogTable = Table.LoadTable(_client, "ProductCatalog");

            // ********** Add Books *********************
            var book1 = new Document
            {
                ["Id"] = 101,
                ["Title"] = "Book 101 Title",
                ["ISBN"] = "111-1111111111",
                ["Authors"] = new List<string> { "Author 1" },
                ["Price"] = -2, // *** Intentional value. Later used to illustrate scan.
                ["Dimensions"] = "8.5 x 11.0 x 0.5",
                ["PageCount"] = 500,
                ["InPublication"] = true,
                ["ProductCategory"] = "Book"
            };
            await productCatalogTable.PutItemAsync(book1);

            var book2 = new Document
            {
                ["Id"] = 102,
                ["Title"] = "Book 102 Title",
                ["ISBN"] = "222-2222222222",
                ["Authors"] = new List<string> { "Author 1", "Author 2" },
                ["Price"] = 20,
                ["Dimensions"] = "8.5 x 11.0 x 0.8",
                ["PageCount"] = 600,
                ["InPublication"] = true,
                ["ProductCategory"] = "Book"
            };
            await productCatalogTable.PutItemAsync(book2);

            var book3 = new Document
            {
                ["Id"] = 103,
                ["Title"] = "Book 103 Title",
                ["ISBN"] = "333-3333333333",
                ["Authors"] = new List<string> { "Author 1", "Author2", "Author 3" },
                ["Price"] = 2000,
                ["Dimensions"] = "8.5 x 11.0 x 1.5",
                ["PageCount"] = 700,
                ["InPublication"] = false,
                ["ProductCategory"] = "Book",
            };
            await productCatalogTable.PutItemAsync(book3);

            // ************ Add bikes. *******************
            var bicycle1 = new Document
            {
                ["Id"] = 201,
                ["Title"] = "18-Bike 201", // size, followed by some title.
                ["Description"] = "201 description",
                ["BicycleType"] = "Road",
                ["Brand"] = "Brand-Company A", // Trek, Specialized.
                ["Price"] = 100,
                ["Color"] = new List<string> { "Red", "Black" },
                ["ProductCategory"] = "Bike"
            };
            await productCatalogTable.PutItemAsync(bicycle1);

            var bicycle2 = new Document
            {
                ["Id"] = 202,
                ["Title"] = "21-Bike 202Brand-Company A",
                ["Description"] = "202 description",
                ["BicycleType"] = "Road",
                ["Brand"] = "",
                ["Price"] = 200,
                ["Color"] = new List<string> { "Green", "Black" },
                ["ProductCategory"] = "Bicycle"
            };
            await productCatalogTable.PutItemAsync(bicycle2);

            var bicycle3 = new Document
            {
                ["Id"] = 203,
                ["Title"] = "19-Bike 203",
                ["Description"] = "203 description",
                ["BicycleType"] = "Road",
                ["Brand"] = "Brand-Company B",
                ["Price"] = 300,
                ["Color"] = new List<string> { "Red", "Green", "Black" },
                ["ProductCategory"] = "Bike"
            };
            await productCatalogTable.PutItemAsync(bicycle3);

            var bicycle4 = new Document
            {
                ["Id"] = 204,
                ["Title"] = "18-Bike 204",
                ["Description"] = "204 description",
                ["BicycleType"] = "Mountain",
                ["Brand"] = "Brand-Company B",
                ["Price"] = 400,
                ["Color"] = new List<string> { "Red" },
                ["ProductCategory"] = "Bike"
            };
            await productCatalogTable.PutItemAsync(bicycle4);

            var bicycle5 = new Document
            {
                ["Id"] = 205,
                ["Title"] = "20-Title 205",
                ["Description"] = "205 description",
                ["BicycleType"] = "Hybrid",
                ["Brand"] = "Brand-Company C",
                ["Price"] = 500,
                ["Color"] = new List<string> { "Red", "Black" },
                ["ProductCategory"] = "Bike"
            };
            await productCatalogTable.PutItemAsync(bicycle5);
        }

        private async Task LoadSampleForums()
        {
            var forumTable = Table.LoadTable(_client, "Forum");

            var forum1 = new Document
            {
                ["Name"] = "Amazon DynamoDB", // PK
                ["Category"] = "Amazon Web Services",
                ["Threads"] = 2,
                ["Messages"] = 4,
                ["Views"] = 1000
            };

            await forumTable.PutItemAsync(forum1);

            var forum2 = new Document
            {
                ["Name"] = "Amazon S3", // PK
                ["Category"] = "Amazon Web Services",
                ["Threads"] = 1
            };

            await forumTable.PutItemAsync(forum2);
        }

        private async Task LoadSampleThreads()
        {
            var threadTable = Table.LoadTable(_client, "Thread");

            // Thread 1.
            var thread1 = new Document
            {
                ["ForumName"] = "Amazon DynamoDB", // Hash attribute.
                ["Subject"] = "DynamoDB Thread 1", // Range attribute.
                ["Message"] = "DynamoDB thread 1 message text",
                ["LastPostedBy"] = "User A",
                ["LastPostedDateTime"] = DateTime.UtcNow.Subtract(new TimeSpan(14, 0, 0, 0)),
                ["Views"] = 0,
                ["Replies"] = 0,
                ["Answered"] = false,
                ["Tags"] = new List<string> { "index", "primarykey", "table" }
            };

            await threadTable.PutItemAsync(thread1);

            // Thread 2.
            var thread2 = new Document
            {
                ["ForumName"] = "Amazon DynamoDB", // Hash attribute.
                ["Subject"] = "DynamoDB Thread 2", // Range attribute.
                ["Message"] = "DynamoDB thread 2 message text",
                ["LastPostedBy"] = "User A",
                ["LastPostedDateTime"] = DateTime.UtcNow.Subtract(new TimeSpan(21, 0, 0, 0)),
                ["Views"] = 0,
                ["Replies"] = 0,
                ["Answered"] = false,
                ["Tags"] = new List<string> { "index", "primarykey", "rangekey" }
            };

            await threadTable.PutItemAsync(thread2);

            // Thread 3.
            var thread3 = new Document
            {
                ["ForumName"] = "Amazon S3", // Hash attribute.
                ["Subject"] = "S3 Thread 1", // Range attribute.
                ["Message"] = "S3 thread 3 message text",
                ["LastPostedBy"] = "User A",
                ["LastPostedDateTime"] = DateTime.UtcNow.Subtract(new TimeSpan(7, 0, 0, 0)),
                ["Views"] = 0,
                ["Replies"] = 0,
                ["Answered"] = false,
                ["Tags"] = new List<string> { "largeobjects", "multipart upload" }
            };

            await threadTable.PutItemAsync(thread3);
        }

        private async Task LoadSampleReplies()
        {
            var replyTable = Table.LoadTable(_client, "Reply");

            // Reply 1 - thread 1.
            var thread1Reply1 = new Document
            {
                ["Id"] = "Amazon DynamoDB#DynamoDB Thread 1", // Hash attribute.
                ["ReplyDateTime"] = DateTime.UtcNow.Subtract(new TimeSpan(21, 0, 0, 0)), // Range attribute.
                ["Message"] = "DynamoDB Thread 1 Reply 1 text",
                ["PostedBy"] = "User A"
            };

            await replyTable.PutItemAsync(thread1Reply1);

            // Reply 2 - thread 1.
            var thread1reply2 = new Document
            {
                ["Id"] = "Amazon DynamoDB#DynamoDB Thread 1", // Hash attribute.
                ["ReplyDateTime"] = DateTime.UtcNow.Subtract(new TimeSpan(14, 0, 0, 0)), // Range attribute.
                ["Message"] = "DynamoDB Thread 1 Reply 2 text",
                ["PostedBy"] = "User B"
            };

            await replyTable.PutItemAsync(thread1reply2);

            // Reply 3 - thread 1.
            var thread1Reply3 = new Document
            {
                ["Id"] = "Amazon DynamoDB#DynamoDB Thread 1", // Hash attribute.
                ["ReplyDateTime"] = DateTime.UtcNow.Subtract(new TimeSpan(7, 0, 0, 0)), // Range attribute.
                ["Message"] = "DynamoDB Thread 1 Reply 3 text",
                ["PostedBy"] = "User B"
            };

            await replyTable.PutItemAsync(thread1Reply3);

            // Reply 1 - thread 2.
            var thread2Reply1 = new Document
            {
                ["Id"] = "Amazon DynamoDB#DynamoDB Thread 2", // Hash attribute.
                ["ReplyDateTime"] = DateTime.UtcNow.Subtract(new TimeSpan(7, 0, 0, 0)), // Range attribute.
                ["Message"] = "DynamoDB Thread 2 Reply 1 text",
                ["PostedBy"] = "User A"
            };


            await replyTable.PutItemAsync(thread2Reply1);

            // Reply 2 - thread 2.
            var thread2Reply2 = new Document
            {
                ["Id"] = "Amazon DynamoDB#DynamoDB Thread 2", // Hash attribute.
                ["ReplyDateTime"] = DateTime.UtcNow.Subtract(new TimeSpan(1, 0, 0, 0)), // Range attribute.
                ["Message"] = "DynamoDB Thread 2 Reply 2 text",
                ["PostedBy"] = "User A"
            };

            await replyTable.PutItemAsync(thread2Reply2);
        }
    }
}
