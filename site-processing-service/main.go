package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoCollection struct {
	collection *mongo.Collection
}

type KafkaClient struct {
	client *kgo.Client
}

type SiteRecord struct {
	URL           string `bson:"url"`
	Hash          string `bson:"hash"`
	LastUpdated   int    `bson:"last_updated"`
	BackLinkCount int    `bson:"back_link_count"`
	Content       string `bson:"content"`
}

func processSiteFromQueue(siteData string, mongoClient *MongoCollection, kafkaClient *kgo.Client) {
	textLines := strings.Split(siteData, "\n")
	var bodyStart int
	var bodyEnd int
	for idx, line := range textLines {
		if strings.Contains(line, "<body>") {
			bodyStart = idx
		} else if strings.Contains(line, "</body>") {
			bodyEnd = idx
		} else if strings.Contains(line, "href") {
			fmt.Println("Found link: ", line)
			addLinkToFetchQueue(line)
		}
	}

	record := SiteRecord{


}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	seeds := []string{os.Getenv("KAFKA_BROKER")}

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("site-fetching-service"),
		kgo.ConsumeTopics("websites"),
	)
	if err != nil {
		panic(err)
	}
	defer kafkaClient.Close()

	ctx := context.Background()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	if err != nil {
		log.Fatal("failed to connect to mongo: %v\n", err)
	}
	defer mongoClient.Disconnect(ctx)

	sitesCollection := MongoCollection{collection: mongoClient.Database(os.Getenv("MONGO_DB_NAME")).Collection(os.Getenv("MONGO_COLLECTION_NAME"))}

	var wg sync.WaitGroup
	// record := &kgo.Record{Topic: "kafka-test", Value: []byte("hello world")}
	// kafkaClient.Produce(ctx, record, func(r *kgo.Record, err error) {
	// 	defer wg.Done()
	// 	if err != nil {
	// 		log.Fatal("record produce failed: %v\n", err)
	// 	}
	// })

	for {
		fetches := kafkaClient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Fatal("fetches had errors: %v\n", errs)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			fmt.Println("received record")
			record := iter.Next()
			processSiteFromQueue(string(record.Value), &sitesCollection, kafkaClient)
		}

	}
}
