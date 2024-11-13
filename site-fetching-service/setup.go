package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Connections struct {
	s3Client    *s3.Client
	kafkaClient *kgo.Client
	mongoClient *mongo.Client
}

func setup(ctx context.Context) Connections {
	/*
		AWS CLIENT SETUP
	*/

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal("failed to aws load configuration, %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	log.Println("S3 client connected")

	/*
		KAFKA CLIENT SETUP
	*/
	seeds := []string{os.Getenv("KAFKA_BROKER")}

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("site-fetching-service"),
		kgo.ConsumeTopics(os.Getenv("KAFKA_TOPIC_FETCH")),
	)
	if err != nil {
		panic(err)
	}

	log.Println("Kafka client connected")

	/*
		MONGO CLIENT SETUP
	*/

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	if err != nil {
		log.Fatal("failed to connect to mongo: %v\n", err)
	}
	log.Println("Mongo client connected")

	return Connections{
		s3Client:    s3Client,
		kafkaClient: kafkaClient,
		mongoClient: mongoClient,
	}
}
