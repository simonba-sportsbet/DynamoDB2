using System;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDB2
{
    public static class DynamoDbDocExt
    {

        public static void Pack(this Document doc, string name, string value) { if (!string.IsNullOrWhiteSpace(value)) doc[name] = value; }
        public static void Pack(this Document doc, string name, int? value) { if (value.HasValue) Pack(doc, name, value.Value); }
        public static void Pack(this Document doc, string name, int value)  { doc[name] = value; }
        public static void Pack(this Document doc, string name, long? value) { if (value.HasValue) doc[name] = value.Value; }
        public static void Pack(this Document doc, string name, long value)  { doc[name] = value; }
        public static void Pack(this Document doc, string name, float? value) { if (value.HasValue) doc[name] = value.Value; }
        public static void Pack(this Document doc, string name, float value) { doc[name] = value; }
        public static void Pack(this Document doc, string name, double? value) { if (value.HasValue) doc[name] = value.Value; }
        public static void Pack(this Document doc, string name, double value) { doc[name] = value; }
        public static void Pack(this Document doc, string name, decimal? value) { if (value.HasValue) doc[name] = value.Value; }
        public static void Pack(this Document doc, string name, decimal value) { doc[name] = value; }
        public static void Pack(this Document doc, string name, DateTime? value) { if (value.HasValue) Pack(doc, name, value.Value); }
        public static void Pack(this Document doc, string name, DateTime value) { doc[name] = value.ToUnixTimeSeconds(); }
        public static void Pack<T>(this Document doc, string name, T? value) where T: struct, Enum  { if (value.HasValue) Pack(doc, name,  value.Value); }
        public static void Pack<T>(this Document doc, string name, T value) where T : Enum { doc[name] = value.ToString(); }
        public static void Pack(this Document doc, string name, Guid? value) { if (value.HasValue) Pack(doc, name, value.Value); }
        public static void Pack(this Document doc, string name, Guid value) { doc[name] = value; }

        public static string UnpackString(this Document doc, string name) => doc.ContainsKey(name) ? doc[name].AsString() : null;
        public static int? UnpackIntn(this Document doc, string name) => doc.ContainsKey(name) ? UnpackInt(doc, name) : (int?)null;
        public static int UnpackInt(this Document doc, string name) => doc[name].AsInt();
        public static long? UnpackLongn(this Document doc, string name) => doc.ContainsKey(name) ? UnpackLong(doc, name) : (long?)null;
        public static long UnpackLong(this Document doc, string name) => doc[name].AsLong();
        public static decimal? UnpackDecimaln(this Document doc, string name) => doc.ContainsKey(name) ? UnpackDecimal(doc, name) : (decimal?)null;
        public static decimal UnpackDecimal(this Document doc, string name) => doc[name].AsDecimal();

        public static float? UnpackFloatn(this Document doc, string name) => doc.ContainsKey(name) ? UnpackFloat(doc, name) : (float?)null;
        public static float UnpackFloat(this Document doc, string name) => (float)doc[name].AsDouble();
        public static double? UnpackDoublen(this Document doc, string name) => doc.ContainsKey(name) ? UnpackDouble(doc, name) : (double?)null;
        public static double UnpackDouble(this Document doc, string name) => (float)doc[name].AsDouble();

        public static DateTime? UnpackDateTimen(this Document doc, string name) => doc.ContainsKey(name) ? UnpackDateTime(doc, name) : (DateTime?)null;
        public static DateTime UnpackDateTime(this Document doc, string name) => doc[name].AsLong().FromUnixTimeSeconds();
        public static Guid? UnpackGuidn(this Document doc, string name) => doc.ContainsKey(name) ? UnpackGuid(doc, name) : (Guid?)null;
        public static Guid UnpackGuid(this Document doc, string name) => doc[name].AsGuid();
        public static T? UnpackEnumn<T>(this Document doc, string name) where T : struct, Enum => doc.ContainsKey(name) ? UnpackEnum<T>(doc, name) : (T?)null;
        public static T UnpackEnum<T>(this Document doc, string name) where T : struct, Enum => Enum.Parse<T>(doc[name].AsString());

    }
}
