using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Assignment7
{
    public class Assignment5
    {
        public string itemId;
        public string type;
        public double rating;
    }

    public class RatingsByType
    {
        public string type;
        public double count;
        public double rating;
        public double average;
    }


    public class Function
    {

        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();

        public async Task<List<Assignment5>> FunctionHandler(DynamoDBEvent input, ILambdaContext context)
        {
            Table table = Table.LoadTable(client, "Assignment5");
            Table table2 = Table.LoadTable(client, "RatingsByType");
            List<Assignment5> ratings = new List<Assignment5>();
            List<RatingsByType> ratings2 = new List<RatingsByType>();
            List<DynamoDBEvent.DynamodbStreamRecord> records = (List<DynamoDBEvent.DynamodbStreamRecord>)input.Records;

            if(records.Count > 0)
            {
                DynamoDBEvent.DynamodbStreamRecord record = records[0];

                if(record.EventName.Equals("INSERT"))
                {
                    Document doc = Document.FromAttributeMap(record.Dynamodb.NewImage);
                    Assignment5 givenRating = JsonConvert.DeserializeObject<Assignment5>(doc.ToJson());

                    double average = givenRating.rating;

                    GetItemResponse res = await client.GetItemAsync("RatingsByType", new Dictionary<string, AttributeValue>
                    {
                        {
                            "type", new AttributeValue { S = givenRating.type}
                        }
                    }
                   );

                    Document getDoc = Document.FromAttributeMap(res.Item);
                    RatingsByType myItem = JsonConvert.DeserializeObject<RatingsByType>(getDoc.ToJson());



                    average = ((myItem.count * myItem.average) + givenRating.rating / (myItem.count + 1));

                    var request = new UpdateItemRequest
                    {
                        TableName = "RatingsByType",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            { "type", new AttributeValue{ S = givenRating.type} }
                        },

                        AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                        {
                            {
                                "count",
                                new AttributeValueUpdate{Action = "ADD", Value = new AttributeValue{N = "1"}}
                            },
                            {
                               "total",
                                new AttributeValueUpdate{Action = "ADD", Value = new AttributeValue{N = givenRating.rating.ToString()}}
                            },
                            { 
                                "average",
                                new AttributeValueUpdate{Action = "PUT", Value = new AttributeValue {N = average.ToString("0.#")}}
                            }

                        },
                    };

                   



                    await client.UpdateItemAsync(request);
                }
            }

            return ratings;
        }
    }
}
