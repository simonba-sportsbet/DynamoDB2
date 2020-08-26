using System;
using System.Threading.Tasks;

namespace DynamoDB2
{
    class Program
    {
        static async Task Main()
        {
            //var t1 = new AwsSample();
            //await t1.Run();

            var t2 = new DocumentModel.ActLogTest();
            await t2.Run();
        }
    }
}
