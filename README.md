# DynamoDB2" 
Quick Technical Evaluation of options to use DynamoDB for Autotrader Activity Log.
There are a number of false paths.
Look at the classes in the DocumentModel fodter.
The test class is ActLogTest.

## To run locally
The code currently conencts to a local instance of DynamoDB.
To run locally a local instance use the following docker

  docker run -p 8000:8000 -d --name EvalDynamoDB amazon/dynamodb-local
