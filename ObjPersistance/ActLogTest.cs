using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDB2.ObjPersistance
{
    class ActLogTest
    {
        private AmazonDynamoDBClient _client;

        public async Task Run()
        {
            var clientConfig = new AmazonDynamoDBConfig { ServiceURL = "http://localhost:8000" };
            _client = new AmazonDynamoDBClient(clientConfig);

            try
            {
                await ResetTables();
                await EnsureTables();

                await AddTestBets();
                await GetTestBet();

                await AddTestRecs();
                await GetTestRecs();

                await GetTestRecsAndBets();
            }
            catch (AmazonServiceException e) 
            { 
                Debug.WriteLine(e.Message); 
            }
            catch (Exception ex)
            {
                for (Exception x = ex; x != null; x = x.InnerException)
                    Debug.WriteLine(x.GetType().Name + ": " + x.Message);
            }
        }

        private readonly CreateTableRequest[] _tableCreateReqs = new[]
        {
            new CreateTableRequest
            {
                TableName = "Bet",
                KeySchema = new List<KeySchemaElement>()  
                { 
                    new KeySchemaElement("EventId", KeyType.HASH),
                    new KeySchemaElement("BetId", KeyType.RANGE) 
                },
                AttributeDefinitions = new List<AttributeDefinition>() 
                { 
                    new AttributeDefinition("EventId", ScalarAttributeType.N),
                    new AttributeDefinition("BetId", ScalarAttributeType.S),
                },
                ProvisionedThroughput = new ProvisionedThroughput(1, 5)
            },
            new CreateTableRequest
            {
                TableName = "Recommendation",
                KeySchema = new List<KeySchemaElement>() 
                { 
                    new KeySchemaElement("EventId", KeyType.HASH), 
                    new KeySchemaElement("RecommendationId", KeyType.RANGE) 
                },
                AttributeDefinitions = new List<AttributeDefinition>() 
                {
                    new AttributeDefinition("EventId", ScalarAttributeType.N),
                    new AttributeDefinition("RecommendationId", ScalarAttributeType.S),
                },
                ProvisionedThroughput = new ProvisionedThroughput(1, 5)
            },
        };

        private Task ResetTables() => Task.WhenAll(_tableCreateReqs.Select(x => TableTools.DeleteTable(_client, x.TableName)).ToArray());

        private async Task EnsureTables()
        {
            foreach (var tcr in _tableCreateReqs)
            {
                var response = await _client.CreateTableAsync(tcr);
                await TableTools.WaitTillTableCreated(_client, tcr.TableName, response);
            }
        }

        // ----------------------------------------------------------------------------------------

        private async Task AddBet(Bet bet)
        {
            var ctx = new DynamoDBContext(_client);
            await ctx.SaveAsync(bet);
        }

        private async Task AddTestBets()
        {
            await AddBet(new Bet
            {
                EventId = 123,
                BetId = "01",
                Timestamp = DateTime.Now,
                CorrelationId = Guid.NewGuid(),
                Legs = new List<Leg>
                {
                    new Leg
                    { 
                        LegType = LegType.WIN_ONLY, 
                        Selection = new Selection
                        {
                            SubclassId      = 789,
                            SubClassName    = "TSC",
                            EventTypeId     = 10,
                            EventTypeName   = "NBA",
                            EventId         = 123,
                            EventName       = "Houston Rockets vs Orlando Magic",
                            MarketId        = 787,
                            MarketName      = "Point",
                            SelectionId     = 456,
                            SelectionName   = "James Harden",
                            Price           = new Price { PriceType = PriceType.STARTING_PRICE, DecimalPrice = 2.5m },
                            EventDate      = new DateTime(2020, 03, 14),
                        }
                    }
                }
            });

            await AddBet(new Bet
            {
                EventId = 123,
                BetId = "02",
                Timestamp = DateTime.Now,
                CorrelationId = Guid.NewGuid()
            });

            await AddBet(new Bet
            {
                EventId = 123,
                BetId = "03",
                Timestamp = DateTime.Now,
                CorrelationId = Guid.NewGuid()
            });
        }

        private async Task GetTestBet()
        {
            var ctx = new DynamoDBContext(_client);

            var betKey = new Bet
            {
                EventId = 123,
                BetId = "01"
            };

            var bet = await ctx.LoadAsync(betKey);
            Debug.WriteLine(bet);
        }

        private async Task AddRecommendation(Recommendation rec)
        {
            var ctx = new DynamoDBContext(_client);
            await ctx.SaveAsync(rec);
        }

        private async Task AddTestRecs()
        {
            var rec = new Recommendation
            {
                EventId = 123,
                RecommendationId = Guid.Parse("D0E888E1-E0F2-4DE7-9000-0EB6ED0AB9CE"),
                Timestamp = DateTime.Now,
                CorrelationId = Guid.NewGuid(),
                BetIds = new List<string> { "01", "02", "03" }
            };

            await AddRecommendation(rec);
        }

        private async Task GetTestRecs()
        {
            var ctx = new DynamoDBContext(_client);

            var recKey = new Recommendation
            {
                EventId = 123,
                RecommendationId = Guid.Parse("D0E888E1-E0F2-4DE7-9000-0EB6ED0AB9CE")
            };


            var rec = await ctx.LoadAsync(recKey);
        }

        private async Task GetTestRecsAndBets()
        {
            var ctx = new DynamoDBContext(_client);

            var recKey = new Recommendation
            {
                EventId = 123,
                RecommendationId = Guid.Parse("D0E888E1-E0F2-4DE7-9000-0EB6ED0AB9CE")
            };


            var rec = await ctx.LoadAsync(recKey);

            var betScan = ctx.ScanAsync<Bet>(new[] { new ScanCondition("BetId", ScanOperator.In, rec.BetIds) });

            while (!betScan.IsDone)
                foreach (var bet in await betScan.GetNextSetAsync())
                    Debug.WriteLine(bet.BetId);
        }
    }
}
