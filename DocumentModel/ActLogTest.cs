using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDB2.DocumentModel
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
            var betTable = Table.LoadTable(_client, "Bet");
            await betTable.PutItemAsync(BetSerialiser.PackBet(bet));
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
                EventId = 456,
                BetId = "03",
                Timestamp = DateTime.Now,
                CorrelationId = Guid.NewGuid()
            });
            
            await AddBet(new Bet
            {
                EventId = 123,
                BetId = "04",
                Timestamp = DateTime.Now,
                CorrelationId = Guid.NewGuid()
            });
            
            await AddBet(new Bet
            {
                EventId = 985,
                BetId = "05",
                Timestamp = DateTime.Now,
                CorrelationId = Guid.NewGuid()
            });
        }

        private async Task GetTestBet()
        {
            var betTable = Table.LoadTable(_client, "Bet");

            var doc = await betTable.GetItemAsync(123, "01");

            var bet = BetSerialiser.UnpackBet(doc);
            Debug.WriteLine(bet);
        }

        private async Task AddRecommendation(Recommendation rec)
        {
            var recTable = Table.LoadTable(_client, "Recommendation");
            await recTable.PutItemAsync(RecommendationSerialiser.PackRecommendation(rec));
        }

        private async Task AddTestRecs()
        {
            var rec = new Recommendation
            {
                EventId = 123,
                RecommendationId = Guid.Parse("D0E888E1-E0F2-4DE7-9000-0EB6ED0AB9CE"),
                Timestamp = DateTime.Now,
                CorrelationId = Guid.NewGuid(),
                BetIds = new List<string> { "01", "02", "04" }
            };

            await AddRecommendation(rec);
        }

        private async Task GetTestRecs()
        {
            var recTable = Table.LoadTable(_client, "Recommendation");

            var eventId = 123;
            var recId = Guid.Parse("D0E888E1-E0F2-4DE7-9000-0EB6ED0AB9CE");
            
            var doc = await recTable.GetItemAsync(eventId, recId);
            var rec = RecommendationSerialiser.UnpackRecommendation(doc);

            Debug.WriteLine(rec);
        }

        private async Task GetTestRecsAndBets()
        {
            var eventId = 123;
            var recId = Guid.Parse("D0E888E1-E0F2-4DE7-9000-0EB6ED0AB9CE");

            var recTable = Table.LoadTable(_client, "Recommendation");
            var rd = await recTable.GetItemAsync(eventId, recId);
            var rec = RecommendationSerialiser.UnpackRecommendation(rd);

            var betTable = Table.LoadTable(_client, "Bet");
            var bg = betTable.CreateBatchGet();
            foreach (var betId in rec.BetIds)
                bg.AddKey(rec.EventId, betId);

            await bg.ExecuteAsync();

            var bets = bg.Results.Select(BetSerialiser.UnpackBet).ToList();

            foreach (var bet in bets)
                Debug.WriteLine($"{bet.EventId}, {bet.BetId}, {bet.Legs.FirstOrDefault()?.Selection?.Price?.PriceType.ToString() ?? "-"}");
        }
    }
}
