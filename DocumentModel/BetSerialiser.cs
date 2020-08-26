using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDB2.DocumentModel
{
    public static class BetSerialiser
    {
        public static Document PackPrice(Price price)
        {
            var doc = new Document();


            doc.Pack("PriceType", price.PriceType);
            doc.Pack("DecimalPrice", price.DecimalPrice);
            doc.Pack("Handicap", price.Handicap);
            doc.Pack("CspPrice", price.CspPrice);
            doc.Pack("BoostedPrice", price.BoostedPrice);

            return doc;

        }

        public static Document PackSelection(Selection selection)
        {
            var doc = new Document();

            doc.Pack("SubclassId", selection.SubclassId);
            doc.Pack("SubClassName", selection.SubClassName);
            doc.Pack("EventTypeId", selection.EventTypeId);
            doc.Pack("EventTypeName", selection.EventTypeName);
            doc.Pack("EventId", selection.EventId);
            doc.Pack("EventName", selection.EventName);
            doc.Pack("MarketId", selection.MarketId);
            doc.Pack("MarketName", selection.MarketName);
            doc.Pack("SelectionId", selection.SelectionId);
            doc.Pack("SelectionName", selection.SelectionName);

            if (selection.Price != null)
                doc["Price"] = PackPrice(selection.Price);

            doc.Pack("EventDate", selection.EventDate);
            doc.Pack("MarketTag", selection.MarketTag);

            return doc;
        }

        public static Document PackLeg(Leg leg)
        {
            var doc = new Document();

            doc.Pack("LegType", leg.LegType);

            if (leg.Selection != null)
                doc["Selection"] = PackSelection(leg.Selection);

            return doc;
        }

        public static Document PackBet(Bet bet)
        {
            var doc = new Document();

            doc.Pack("EventId", bet.EventId);
            doc.Pack("BetId", bet.BetId);
            doc.Pack("Timestamp", bet.Timestamp);
            doc.Pack("CorrelationId", bet.CorrelationId);

            doc["Legs"] = bet.Legs?.Select(x => PackLeg(x)).ToList() ?? new List<Document>();

            return doc;
        }

        public static Price UnpackPrice(Document doc) => new Price
        {
            PriceType = doc.UnpackEnum<PriceType>("PriceType"),
            DecimalPrice = doc.UnpackDecimal("DecimalPrice"),
            Handicap = doc.UnpackFloatn("Handicap"),
            CspPrice = doc.UnpackDoublen("CspPrice"),
            BoostedPrice = doc.UnpackDoublen("BoostedPrice")
        };

        public static Selection UnpackSelection(Document doc) => new Selection
        {
            SubclassId = doc.UnpackIntn("SubclassId"),
            SubClassName = doc.UnpackString("SubClassName"),
            EventTypeId = doc.UnpackLongn("EventTypeId"),
            EventTypeName = doc.UnpackString("EventTypeName"),
            EventId = doc.UnpackLongn("EventId"),
            EventName = doc.UnpackString("EventName"),
            MarketId = doc.UnpackLongn("MarketId"),
            MarketName = doc.UnpackString("MarketName"),
            SelectionId = doc.UnpackLongn("SelectionId"),
            SelectionName = doc.UnpackString("SelectionName"),
            EventDate = doc.UnpackDateTimen("EventDate"),
            MarketTag = doc.UnpackString("MarketTag"),
            Price = UnpackPrice(doc["Price"].AsDocument())
        };

        public static Leg UnpackLeg(Document doc) => new Leg
        {
            LegType = doc.UnpackEnum<LegType>("LegType"),
            Selection = UnpackSelection(doc["Selection"].AsDocument())
        };

        public static IList<Leg> UnpackLegs(DynamoDBEntry dBEntry)
            => dBEntry?.AsDynamoDBList().Entries.Select(x => UnpackLeg(x.AsDocument())).ToList();

        public static Bet UnpackBet(Document doc) => new Bet
        {
            EventId = doc.UnpackInt("EventId"),
            BetId = doc.UnpackString("BetId"),
            Timestamp = doc.UnpackDateTime("Timestamp"),
            CorrelationId = doc.UnpackGuid("CorrelationId"),
            Legs = UnpackLegs(doc["Legs"])
        };
    }
}
