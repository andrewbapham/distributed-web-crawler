# Site Fetching Service

This service is responsible for fetching the HTML content of a website, creating/updating the associated record in MongoDB for metadata and uploading the HTML content to S3.

## Development Setup

The following environment variables need to be set (in a `.env` file) for the service to run:
- `MONGO_URL`: The URL to the MongoDB database (e.g. `mongodb://localhost:27017`)
- `MONGO_DB_NAME`: The name of the MongoDB database (e.g. `web-crawler-data`)
- `MONGO_COLLECTION_NAME`: The name of the MongoDB collection to store the website data (e.g. `websites`)
- `S3_BUCKET_NAME`: The name of the S3 bucket to upload the HTML content to (e.g. `web-crawler-html-content`)
- `KAFKA_BROKER_URL`: The URL of the Kafka broker (e.g. `localhost:9092`)
- `KAFKA_TOPIC_FETCH`: The name of the Kafka topic to fetch messages from
- `KAFKA_TOPIC_PROCESS`: The name of the Kafka topic to send off messages to process
- `KAFKA_TOPIC_DLQ`: The name of the Kafka topic to send off messages to the dead letter queue

2 constants `MAX_RETRY_COUNT` and `RETRY_TIMEOUT` are set in the `main.go` file to manage the number of retries and the timeout between retries, these can be adjusted as needed.

## Inputs

The service takes in a message from a Kafka topic that contains the URL of the website to fetch. The message is in the following format:

```json
{
    "link": "https://example.com",
    "retry_count": 0
}
```

The `link` field is the URL of the website to fetch and the `retry_count` field is the number of times the service has tried to fetch the website. This is used to manage retries in case the website is unreachable on the first fetch. After a certain number of retries (`MAX_RETRY_COUNT`), the service will place the message on a dead letter queue (DLQ) for further investigation.

## Outputs

The service outputs a message to another Kafka topic that contains the URL of the website to process. The message is in the following format:

```json
{
    "link": "https://example.com"
}
```