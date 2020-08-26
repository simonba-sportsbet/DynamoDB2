using System;

namespace DynamoDB2
{
    public static class DateTimeExt
    {
        public static long ToUnixTimeSeconds(this DateTime dateTime) => (long)(dateTime - DateTime.UnixEpoch).TotalSeconds;
        public static DateTime FromUnixTimeSeconds(this long dateTime) => DateTime.UnixEpoch.AddSeconds(dateTime);
    }
}
