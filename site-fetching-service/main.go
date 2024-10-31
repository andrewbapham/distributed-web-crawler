package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"log"
	"net/http"
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
)

type MongoCollection struct {
	collection *mongo.Collection
}

type SiteRecord struct {
	URL           string `bson:"url"`
	Hash          string `bson:"hash"`
	LastFetched   int64  `bson:"last_fetched"`
	LastUpdated   int64  `bson:"last_updated"`
	BackLinkCount int    `bson:"back_link_count"`
}

func getS3KeyFromLink(link string) (string, error) {
	// extract host and path from link, e.g.
	// removes protocol and query params
	// https://google.com/search -> google.com/search
	u, err := url.Parse(link)
	if err != nil {
		return "", err
	}
	return u.Host + u.Path, nil
}

func getWebsiteData(url string) string {
	// fetch website data
	if !strings.HasPrefix(url, "http") || !strings.HasPrefix(url, "https") {
		url = "https://" + url
	}
	res, err := http.Get(url)
	if err != nil {
		log.Fatal("failed to fetch website data: %v", err)
	}
	content, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal("failed to read website data: %v", err)
	}
	return string(content)
}

func uploadToS3(html string, key string, s3Client *s3.Client) {
	// upload to S3
	_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(os.Getenv("S3_BUCKET_NAME")),
		Key:    aws.String(key),
		Body:   strings.NewReader(html),
	})
	if err != nil {
		log.Fatal("failed to upload to S3: %v", err)
	}

	log.Printf("uploaded html from link: %v to S3\n", key)
}

func handleSiteFromQueue(url string, mongoCollection *mongo.Collection, kafkaClient *kgo.Client, s3Client *s3.Client) {
	// fetch website data from kafka queue
	html := getWebsiteData(url)

	h := sha256.New()
	h.Write([]byte(html))
	htmlHash := base64.URLEncoding.EncodeToString(h.Sum(nil))

	// check if URL and hash already exist in database, and if not, insert and add S3 object
	// if hash is different, update the hash and S3 object
	log.Println("hash: ", htmlHash)

	newRecord := SiteRecord{
		URL:         url,
		Hash:        htmlHash,
		LastFetched: time.Now().Unix(),
	}

	var existingRecord SiteRecord
	res := mongoCollection.FindOne(context.Background(), bson.D{{Key: "url", Value: url}})
	if res.Err() != nil && errors.Is(res.Err(), mongo.ErrNoDocuments) {
		// record not found, insert to database
		log.Println("record not found")
		result, err := mongoCollection.InsertOne(context.Background(), newRecord)
		if err != nil {
			log.Println("failed to insert record: %v", err)
		} else {
			log.Println("record inserted into mongo with id: ", result.InsertedID)
			// upload to S3
			uploadToS3(html, url, s3Client)
		}

	} else if res.Err() != nil {
		log.Fatal("record fetch failed: %v", res.Err())

	} else {
		// record found
		res.Decode(&existingRecord)

		if existingRecord.Hash != htmlHash {
			log.Println("Hashes don't match, updating...")
			// update hash
			valuesToUpdate := bson.D{{Key: "hash", Value: htmlHash}, {Key: "last_fetched", Value: time.Now().Unix()}}
			_, err := mongoCollection.UpdateOne(context.Background(), bson.D{{Key: "url", Value: url}}, bson.D{{Key: "$set", Value: valuesToUpdate}})

			if err != nil {
				log.Fatal("failed to update record: %v", err)
			}

			// upload to S3
			uploadToS3(html, url, s3Client)
		}
	}

}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

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
		kgo.ConsumeTopics("websites"),
	)
	if err != nil {
		panic(err)
	}
	defer kafkaClient.Close()

	log.Println("Kafka client connected")

	ctx := context.Background()

	/*
		MONGO CLIENT SETUP
	*/

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	if err != nil {
		log.Fatal("failed to connect to mongo: %v\n", err)
	}
	defer mongoClient.Disconnect(ctx)

	log.Println("Mongo client connected")

	sitesCollection := mongoClient.Database(os.Getenv("MONGO_DB_NAME")).Collection(os.Getenv("MONGO_COLLECTION_NAME"))

	// Listen to incoming site queue and fetch data on new sites
	for {
		fetches := kafkaClient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Fatal("fetches had errors: %v\n", errs)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			log.Println("received record: " + string(record.Value))

			url, err := getS3KeyFromLink(string(record.Value))
			if err != nil {
				log.Printf("failed to parse URL: %v for record: %v, skipping...\n", err, string(record.Value))
				continue
			}
			handleSiteFromQueue(url, sitesCollection, kafkaClient, s3Client)
		}
	}
}
