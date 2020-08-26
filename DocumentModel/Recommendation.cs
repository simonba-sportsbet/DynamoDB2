using System;
using System.Collections.Generic;

namespace DynamoDB2.DocumentModel
{
    public class Recommendation
    {
        public int EventId { get; set; }
        public Guid RecommendationId { get; set; }

        public DateTime Timestamp { get; set; }
        public Guid CorrelationId { get; set; }
        public List<string> BetIds { get; set; }
    }
}
