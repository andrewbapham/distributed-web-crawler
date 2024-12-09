package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/html"
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

type SiteLink struct {
	Host string
	Path string
}

func (s SiteLink) String() string {
	return s.Host + s.Path
}

func getS3KeyFromLink(link string) (SiteLink, error) {
	// extract host and path from link, e.g.
	// removes protocol and query params
	// https://google.com/search -> google.com/search
	u, err := url.Parse(link)
	if err != nil {
		return SiteLink{}, err
	}
	return SiteLink{Host: u.Host, Path: u.Path}, nil
}

func getSiteDataFromS3(key string, s3Client *s3.Client) (io.ReadCloser, error) {
	// get raw html from s3
	result, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("S3_BUCKET_NAME")),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	return result.Body, nil
}

func addLinkToFetchQueue(link string, kafkaClient *kgo.Client) {
	fmt.Println("Adding link to fetch queue: ", link)
	kafkaClient.Produce(context.Background(), &kgo.Record{
		Topic: os.Getenv("KAFKA_TOPIC_FETCH"),
		Value: []byte(link),
	}, func(record *kgo.Record, err error) {
		if err != nil {
			log.Printf("failed to produce record: %v\n", err)
		}
	},
	)
}

func isRelativeLink(link string) bool {
	return strings.HasPrefix(link, "/") || strings.HasPrefix(link, "./")
}

func toActualLink(siteLink *SiteLink, link string) string {
	if isRelativeLink(link) {
		link = strings.TrimPrefix(link, ".")
		return siteLink.Host + link
	}
	return link
}

func processHtml(htmlTokenizer *html.Tokenizer, siteLink *SiteLink, kafkaClient *kgo.Client) string {
	// parse html
	previousStartToken := htmlTokenizer.Token()
	textContent := ""
tokenParseLoop:
	for {
		tokenType := htmlTokenizer.Next()
		if tokenType == html.ErrorToken {
			break tokenParseLoop
		}
		if tokenType == html.StartTagToken {
			token := htmlTokenizer.Token()
			previousStartToken = token
			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						actualLink := toActualLink(siteLink, attr.Val)

						fmt.Println("Found link: ", attr.Val, " -> ", actualLink)
						addLinkToFetchQueue(actualLink, kafkaClient)
					}
				}
			} else if token.Data == "br" {
				textContent += "\n"
			}
		} else if tokenType == html.TextToken {
			if previousStartToken.Data == "script" || previousStartToken.Data == "style" {
				continue
			}
			tokenTextContent := strings.TrimSpace(html.UnescapeString(string(htmlTokenizer.Text())))
			if tokenTextContent != "" {
				textContent += tokenTextContent + " "
			}
		}
	}
	return textContent
}

func processSiteFromQueue(link string, mongoClient *MongoCollection, kafkaClient *kgo.Client, s3Client *s3.Client) {
	fmt.Println("Processing site: ", link)
	siteLink, err := getS3KeyFromLink(link)
	if err != nil {
		log.Printf("failed to parse URL: %v, skipping...\n", err)
		return
	}

	// get raw html from s3
	s3Result, err := getSiteDataFromS3(siteLink.String(), s3Client)
	if err != nil {
		log.Printf("failed to get site data from S3: %v, skipping...\n", err)
		return
	}
	defer s3Result.Close()

	htmlTokenizer := html.NewTokenizer(s3Result)

	textContent := processHtml(htmlTokenizer, &siteLink, kafkaClient)
	fmt.Println("extracted text content: ", textContent)

	// update site record in mongo
	res, err := mongoClient.collection.UpdateOne(
		context.Background(),
		bson.D{{Key: "url", Value: siteLink.String()}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "content", Value: textContent},
			{Key: "last_updated", Value: time.Now().Unix()},
		}}},
	)
	if err != nil {
		log.Fatalf("failed to update record: %v", err)
	}

	if res.MatchedCount == 0 {
		log.Fatalf("record not found for link %v", siteLink.String())
	}

	fmt.Println("updated site record for site ", link)
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	ctx := context.Background()

	//s3 connection
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("failed to aws load configuration, %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	fmt.Println("S3 client connected")

	seeds := []string{os.Getenv("KAFKA_BROKER")}

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("site-fetching-service"),
		kgo.ConsumeTopics(os.Getenv("KAFKA_TOPIC_PROCESS")),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("Kafka client connected")
	defer kafkaClient.Close()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	if err != nil {
		log.Fatalf("failed to connect to mongo: %v\n", err)
	}
	fmt.Println("Mongo client connected")
	defer mongoClient.Disconnect(ctx)

	sitesCollection := MongoCollection{collection: mongoClient.Database(os.Getenv("MONGO_DB_NAME")).Collection(os.Getenv("MONGO_COLLECTION_NAME"))}

	for {
		fetches := kafkaClient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Fatalf("fetches had errors: %v\n", errs)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			fmt.Println("received record")
			record := iter.Next()
			processSiteFromQueue(string(record.Value), &sitesCollection, kafkaClient, s3Client)
		}

	}
}
