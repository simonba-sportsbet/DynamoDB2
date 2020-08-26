using System;
using System.Collections.Generic;

namespace DynamoDB2.DocumentModel
{
    public enum LegType
    {
        EACH_WAY,
        WIN_ONLY,
        PLACE
    }
    public enum PriceType
    {
        LIVE_PRICE,
        STARTING_PRICE,
        GUARANTEED_PRICE,
        DIVIDEND,
        NUMBERS,
        ANTE_POST,
        BOOSTED_PRICE
    }

    public class Price
    {
        public PriceType PriceType { get; set; }
        public decimal DecimalPrice { get; set; }
        public float? Handicap { get; set; }
        public double? CspPrice { get; set; }
        public double? BoostedPrice { get; set; }
    }

    public class Selection
    {
        public int? SubclassId { get; set; }
        public string SubClassName { get; set; }
        public long? EventTypeId { get; set; }
        public string EventTypeName { get; set; }
        public long? EventId { get; set; }
        public string EventName { get; set; }
        public long? MarketId { get; set; }
        public string MarketName { get; set; }
        public long? SelectionId { get; set; }
        public string SelectionName { get; set; }
        public Price Price { get; set; }
        public DateTime? EventDate { get; set; }
        public string MarketTag { get; set; }
    }

    public class Leg
    {
        public bool? SameGameMulti { get; set; }
        public Selection Selection { get; set; }
        public LegType LegType { get; set; }
    }

    public class Bet
    {
        public int EventId { get; set; }
        public  string BetId { get; set; }
        public DateTime Timestamp { get; set; }
        public Guid CorrelationId { get; set; }
        public IList<Leg> Legs { get; set; }
    }
}