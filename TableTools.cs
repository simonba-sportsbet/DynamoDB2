using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDB2
{
    public static class TableTools
    {
        public static async Task<bool> TableExists(AmazonDynamoDBClient client, string tableName)
        {
            try
            {
                var res = await client.DescribeTableAsync(new DescribeTableRequest { TableName = tableName });
                string status = res.Table.TableStatus;

                return string.Equals(status, "ACTIVE");
            }
            catch(ResourceNotFoundException ex)
            {
                Debug.WriteLine(ex.GetType().Name + " - " + ex.Message);
                return false;
            }
        }

        public static async Task WaitTillTableCreated(AmazonDynamoDBClient client, string tableName, CreateTableResponse response)
        {
            var tableDescription = response.TableDescription;

            string status = tableDescription.TableStatus;

            Console.WriteLine(tableName + " - " + status);

            // Let us wait until table is created. Call DescribeTable.
            while (status != "ACTIVE")
            {
                System.Threading.Thread.Sleep(5000); // Wait 5 seconds.
                try
                {
                    var res = await client.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = tableName
                    });
                    Console.WriteLine("Table name: {0}, status: {1}", res.Table.TableName, res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }
                // Try-catch to handle potential eventual-consistency issue.
                catch (ResourceNotFoundException exRnf)
                {
                    for (Exception ex = exRnf; ex != null; ex = ex.InnerException)
                        Debug.WriteLine(ex.GetType().Name + " - " + ex.Message);
                }
            }
        }

        public static async Task DeleteTable(AmazonDynamoDBClient client, string tableName)
        {
            try
            {
                var response = await client.DeleteTableAsync(new DeleteTableRequest
                {
                    TableName = tableName
                });
                await WaitTillTableDeleted(client, tableName, response);
            }
            catch (ResourceNotFoundException exRnf)
            {
                // There is no such table.
                for (Exception ex = exRnf; ex != null; ex = ex.InnerException)
                    Debug.WriteLine(ex.GetType().Name + " - " + ex.Message);
            }
        }

        public static async Task WaitTillTableDeleted(AmazonDynamoDBClient client, string tableName, DeleteTableResponse response)
        {
            var tableDescription = response.TableDescription;

            string status = tableDescription.TableStatus;

            Console.WriteLine("Waiting for delete - " + tableName + " : " + status);

            // Let us wait until table is created. Call DescribeTable
            try
            {
                while (status == "DELETING")
                {
                    System.Threading.Thread.Sleep(5000); // wait 5 seconds

                    var res = await client.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = tableName
                    });
                    Console.WriteLine("Table name: {0}, status: {1}", res.Table.TableName, res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }
            }
            catch (ResourceNotFoundException exRnf)
            {
                for (Exception ex = exRnf; ex != null; ex = ex.InnerException)
                    Debug.WriteLine(ex.GetType().Name + " - " + ex.Message);
            }
        }
    }
}
