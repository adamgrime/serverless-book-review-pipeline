using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using System.Text.Json;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

public class Function
{
    private readonly IAmazonS3 _s3Client = new AmazonS3Client();
    private readonly IAmazonSimpleNotificationService _snsClient = new AmazonSimpleNotificationServiceClient();

    private readonly string _processedBucketName = Environment.GetEnvironmentVariable("PROCESSED_BUCKET_NAME");
    private readonly string _processedTopicArn = Environment.GetEnvironmentVariable("PROCESSED_TOPIC_ARN");

    public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
    {
        context.Logger.LogInformation($"Received {sqsEvent.Records.Count} SQS messages for processing.");

        foreach (var message in sqsEvent.Records)
        {
            var s3Notification = JsonSerializer.Deserialize<Amazon.S3.Util.S3EventNotification>(message.Body);

            var s3Record = s3Notification?.Records[0].S3;

            if (s3Record != null)
            {
                var bucketName = s3Record.Bucket.Name; // Raw bucket name
                var objectKey = s3Record.Object.Key;   // File path/key

                context.Logger.LogInformation($"Starting ETL for file: {objectKey} from bucket: {bucketName}");

                try
                {
                    string rawContent = await ReadFromS3Async(bucketName, objectKey);

                    string processedContent = ProcessReviewData(rawContent);

                    var processedKey = $"clean/{objectKey.Replace("raw/", "")}";
                    await WriteToS3Async(_processedBucketName, processedKey, processedContent);

                    // Notify downstream services
                    var snsMessage = $"File processed: {processedKey}. Source: {objectKey}";
                    await _snsClient.PublishAsync(new PublishRequest
                    {
                        TopicArn = _processedTopicArn,
                        Message = snsMessage,
                        Subject = "Book Review Processed Successfully"
                    });

                    context.Logger.LogInformation($"Successfully completed ETL and published SNS for {objectKey}");
                }
                catch (Exception ex)
                {
                    context.Logger.LogError($"Failed to process {objectKey}. Error: {ex.Message}");
                    // Return message to SQS for retry.
                    throw;
                }
            }
        }
    }

    private async Task<string> ReadFromS3Async(string bucketName, string key)
    {
        using var response = await _s3Client.GetObjectAsync(new GetObjectRequest { BucketName = bucketName, Key = key });
        using var reader = new StreamReader(response.ResponseStream);
        return await reader.ReadToEndAsync();
    }

    private async Task WriteToS3Async(string bucketName, string key, string content)
    {
        await _s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            ContentBody = content
        });
    }

    private string ProcessReviewData(string rawData)
    {
        int reviewCount = rawData.Split('\n').Length;
        return $"Total reviews processed: {reviewCount}. Data validation placeholder complete.";
    }
}