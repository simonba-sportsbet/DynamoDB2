using System;
using System.Collections.Generic;
using System.Text;
using Amazon.DynamoDBv2.DataModel;

namespace DynamoDB2.ObjPersistance
{
    [DynamoDBTable("Recommendation")]
    public class Recommendation
    {
        [DynamoDBHashKey] 
        public int EventId { get; set; }

        [DynamoDBRangeKey] 
        public Guid RecommendationId { get; set; }

        public DateTime Timestamp { get; set; }
        public Guid CorrelationId { get; set; }
        public List<string> BetIds { get; set; }
    }
}
