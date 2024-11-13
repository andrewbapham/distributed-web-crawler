package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoCollection struct {
	collection *mongo.Collection
}

type SiteKafkaMessage struct {
	Link       string `json:"link"`
	RetryCount int    `json:"retry_count"`
}

type SiteMongoRecord struct {
	URL           string `bson:"url"`
	Hash          string `bson:"hash"`
	LastFetched   int64  `bson:"last_fetched"`
	LastUpdated   int64  `bson:"last_updated"`
	BackLinkCount int    `bson:"back_link_count"`
}

const (
	MAX_RETRY_COUNT = 5
	RETRY_TIMEOUT   = 5 * time.Second
)

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

func generateHash(html string) string {
	h := sha256.New()
	h.Write([]byte(html))
	return base64.URLEncoding.EncodeToString(h.Sum(nil))
}

func getWebsiteData(url string) (string, error) {
	// fetch website data
	if !strings.HasPrefix(url, "http") || !strings.HasPrefix(url, "https") {
		url = "https://" + url
	}
	res, err := http.Get(url)
	if err != nil || res.StatusCode == 404 {
		return "", errors.New("failed to fetch website data")
	}
	content, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatalf("failed to read website data: %v", err)
	}
	return string(content), nil
}

func uploadToS3(html string, key string, s3Client *s3.Client) {
	// upload to S3
	_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(os.Getenv("S3_BUCKET_NAME")),
		Key:    aws.String(key),
		Body:   strings.NewReader(html),
	})
	if err != nil {
		log.Fatalf("failed to upload to S3: %v", err)
	}

	log.Printf("uploaded html from link: %v to S3\n", key)
}

func addSiteToDeadLetterQueue(site *SiteKafkaMessage, kafkaClient *kgo.Client) {
	// add link to dead letter queue
	kafkaClient.Produce(context.Background(), &kgo.Record{
		Topic: os.Getenv("KAFKA_TOPIC_DLQ"),
		Value: []byte(site.Link),
	}, func(r *kgo.Record, err error) {
		if err != nil {
			log.Fatalf("failed to produce record: %v", err)
		} else {
			log.Printf("added link: %v to dead letter queue\n", site.Link)
		}
	},
	)
}

func addSiteToRetryQueue(site *SiteKafkaMessage, kafkaClient *kgo.Client) {
	// add link to retry queue
	site.RetryCount++
	if site.RetryCount > MAX_RETRY_COUNT {
		log.Printf("retry count exceeded for link: %v, adding to dead letter queue\n", site.Link)
		addSiteToDeadLetterQueue(site, kafkaClient)
		return
	}

	messageJson, err := json.Marshal(site)
	if err != nil {
		log.Fatalf("failed to marshal site data: %v", err)
	}
	kafkaClient.Produce(context.Background(), &kgo.Record{
		Topic: os.Getenv("KAFKA_TOPIC_FETCH"),
		Value: []byte(messageJson),
	}, func(r *kgo.Record, err error) {
		if err != nil {
			log.Fatalf("failed to produce record: %v", err)
		} else {
			log.Printf("added link: %v back to fetch queue\n", site.Link)
		}
	},
	)
}

func unmarshalSiteKafkaMessage(record []byte) (SiteKafkaMessage, error) {
	var message SiteKafkaMessage
	err := json.Unmarshal(record, &message)
	if err != nil {
		return SiteKafkaMessage{}, err
	}
	return message, nil
}

func uploadOrUpdateSiteRecord(html string, htmlHash string, url string, mongoCollection *mongo.Collection, s3Client *s3.Client) {
	// check if URL and hash already exist in database, and if not, insert and add S3 object
	// if hash is different, update the hash and S3 object
	newRecord := SiteMongoRecord{
		URL:         url,
		Hash:        htmlHash,
		LastFetched: time.Now().Unix(),
	}

	var existingRecord SiteMongoRecord
	res := mongoCollection.FindOne(context.Background(), bson.D{{Key: "url", Value: url}})
	if res.Err() != nil && errors.Is(res.Err(), mongo.ErrNoDocuments) {
		// record not found, insert to database
		log.Println("record not found")
		result, err := mongoCollection.InsertOne(context.Background(), newRecord)
		if err != nil {
			log.Println("failed to insert record: ", err)
		} else {
			log.Println("record inserted into mongo with id: ", result.InsertedID)
			// upload to S3
			uploadToS3(html, url, s3Client)
		}
		return
	} else if res.Err() != nil {
		log.Fatal("record fetch failed: ", res.Err())
		return
	}

	// record found
	res.Decode(&existingRecord)

	if existingRecord.Hash != htmlHash {
		log.Println("Hashes don't match, updating...")
		// update hash
		valuesToUpdate := bson.D{{Key: "hash", Value: htmlHash}, {Key: "last_fetched", Value: time.Now().Unix()}}
		_, err := mongoCollection.UpdateOne(context.Background(), bson.D{{Key: "url", Value: url}}, bson.D{{Key: "$set", Value: valuesToUpdate}})

		if err != nil {
			log.Fatalf("failed to update record: %v", err)
		}

		// upload to S3
		uploadToS3(html, url, s3Client)
	}

}

func handleSiteFromQueue(site *SiteKafkaMessage, mongoCollection *mongo.Collection, kafkaClient *kgo.Client, s3Client *s3.Client) {
	url, err := getS3KeyFromLink(site.Link)
	if err != nil {
		log.Printf("failed to parse URL: %v, skipping...\n", err)
		return
	}
	// fetch website data from kafka queue
	html, err := getWebsiteData(url)
	if err != nil && err.Error() == "failed to fetch website data" {
		log.Printf("failed to fetch website data for link: %v, adding to retry queue\n", url)
		go func() {
			time.Sleep(RETRY_TIMEOUT)
			addSiteToRetryQueue(site, kafkaClient)
		}()
		return
	} else if err != nil {
		log.Fatalf("failed to fetch website data for link: %v: %v\n", url, err)
	}

	htmlHash := generateHash(html)

	uploadOrUpdateSiteRecord(html, htmlHash, url, mongoCollection, s3Client)
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	ctx := context.Background()
	connections := setup(ctx)

	kafkaClient := connections.kafkaClient
	s3Client := connections.s3Client
	mongoClient := connections.mongoClient

	defer kafkaClient.Close()
	defer mongoClient.Disconnect(ctx)

	sitesCollection := mongoClient.Database(os.Getenv("MONGO_DB_NAME")).Collection(os.Getenv("MONGO_COLLECTION_NAME"))

	// Listen to incoming site queue and fetch data on new sites
	for {
		fetches := kafkaClient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Fatalf("fetches had errors: %v", errs)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			log.Println("received record: " + string(record.Value))
			siteData, err := unmarshalSiteKafkaMessage(record.Value)
			if err != nil {
				log.Printf("failed to unmarshal site data: %v for record: %v, skipping...\n", err, string(record.Value))
				continue
			}
			handleSiteFromQueue(&siteData, sitesCollection, kafkaClient, s3Client)
		}
	}
}
