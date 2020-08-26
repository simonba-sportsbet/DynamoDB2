using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDB2.DocumentModel
{
    public static class RecommendationSerialiser
    {
        public static Document PackRecommendation(Recommendation rec)
        {
            var doc = new Document();

            doc.Pack("EventId", rec.EventId);
            doc.Pack("RecommendationId", rec.RecommendationId);
            doc.Pack("CorrelationId", rec.CorrelationId);
            doc.Pack("Timestamp", rec.Timestamp);

            if (rec.BetIds?.Any() ?? false)
                doc["BetIds"] = rec.BetIds;

            return doc;
        }

        public static Recommendation UnpackRecommendation(Document doc)
        {
            var rec = new Recommendation
            {
                EventId = doc.UnpackInt("EventId"),
                RecommendationId = doc.UnpackGuid("RecommendationId"),
                CorrelationId = doc.UnpackGuid("CorrelationId"),
                Timestamp = doc.UnpackDateTime("Timestamp")
            };

            if (doc.ContainsKey("BetIds"))
                rec.BetIds = doc["BetIds"].AsListOfString();

            return rec;
        }
    }
}
