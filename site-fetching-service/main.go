package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.mongodb.org/mongo-driver/bson"
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
	LastFetched   int64  `bson:"last_fetched"`
	LastUpdated   int64  `bson:"last_updated"`
	BackLinkCount int    `bson:"back_link_count"`
}

// mocking kafka stream of websites to visit
var sites = []string{
	"http://thophuongha.com/MuiSuaMe.html",
	"http://thophuongha.com/AnhSangLaDay.html",
	"http://thophuongha.com/XanhMauMat.html",
	"http://thophuongha.com/ThuBruxelles.html",
}

func getWebsiteData(link string) string {
	// fetch website data
	res, err := http.Get(link)
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

func handleSiteFromQueue(link string, mongoClient *MongoCollection, kafkaClient *kgo.Client) {
	// fetch website data from kafka queue
	html := getWebsiteData(link)

	h := sha256.New()
	h.Write([]byte(html))
	htmlHash := base64.URLEncoding.EncodeToString(h.Sum(nil))

	// check if URL and hash already exist in database, and if not, insert and add S3 object
	// if hash is different, update the hash and S3 object
	fmt.Println("hash: ", htmlHash)

	newRecord := SiteRecord{
		URL:         link,
		Hash:        htmlHash,
		LastFetched: time.Now().Unix(),
	}

	var existingRecord SiteRecord
	res := mongoClient.collection.FindOne(context.Background(), bson.D{{"url", link}})
	if res.Err() != nil && errors.Is(res.Err(), mongo.ErrNoDocuments) {
		fmt.Println("record not found")
		result, err := mongoClient.collection.InsertOne(context.Background(), newRecord)
		if err != nil {
			log.Fatal("failed to insert record: %v", err)
		}
		fmt.Println("record inserted: ", result.InsertedID)
	} else if res.Err() != nil {
		log.Fatal("record fetch failed: %v", res.Err())
	} else {
		// record found
		res.Decode(&existingRecord)

		if existingRecord.Hash != htmlHash {
			fmt.Println("Hashes don't match, updating...")
			// update hash
			valuesToUpdate := bson.D{{Key: "hash", Value: htmlHash}, {Key: "last_fetched", Value: time.Now().Unix()}}
			_, err := mongoClient.collection.UpdateOne(context.Background(), bson.D{{Key: "url", Value: link}}, bson.D{{Key: "$set", Value: valuesToUpdate}})

			if err != nil {
				log.Fatal("failed to update record: %v", err)
			}
		}
	}
	//res, _ := bson.MarshalExtJSON(result, false, false)
	//fmt.Println(string(res))

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

	fmt.Println("Kafka client connected")

	ctx := context.Background()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	if err != nil {
		log.Fatal("failed to connect to mongo: %v\n", err)
	}
	defer mongoClient.Disconnect(ctx)

	fmt.Println("Mongo client connected")

	sitesCollection := MongoCollection{collection: mongoClient.Database(os.Getenv("MONGO_DB_NAME")).Collection(os.Getenv("MONGO_COLLECTION_NAME"))}

	// Listen to incoming site queue and fetch data on new sites
	for {
		fetches := kafkaClient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Fatal("fetches had errors: %v\n", errs)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			fmt.Println("received record")
			record := iter.Next()

			url, err := url.ParseRequestURI(string(record.Value))
			if err != nil {
				fmt.Printf("failed to parse URL: %v for record: %v, skipping...\n", err, string(record.Value))
				continue
			}
			handleSiteFromQueue(url.String(), &sitesCollection, kafkaClient)
		}

	}
}
