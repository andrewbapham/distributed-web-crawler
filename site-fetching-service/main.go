package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

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
		fmt.Printf("failed to fetch website data: %v", err)
	}
	content, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		fmt.Printf("failed to read website data: %v", err)
	}
	return string(content)
}
func main() {
	seeds := []string{"localhost:9092"}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("site-fetching-service"),
		kgo.ConsumeTopics("websites"),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	var wg sync.WaitGroup
	// record := &kgo.Record{Topic: "kafka-test", Value: []byte("hello world")}
	// client.Produce(ctx, record, func(r *kgo.Record, err error) {
	// 	defer wg.Done()
	// 	if err != nil {
	// 		fmt.Printf("record produce failed: %v\n", err)
	// 	}
	// })

	for _, site := range sites {
		websiteData := getWebsiteData(site)
		// get first line of websiteData
		firstLine := strings.Join(strings.Split(websiteData, "\n")[0:6], "\n")

		record := &kgo.Record{Topic: "kafka-test", Value: []byte("site: " + site + " - " + firstLine)}
		wg.Add(1)
		client.Produce(ctx, record, func(r *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				fmt.Printf("record produce failed: %v\n", err)
			}
		})
	}

	wg.Wait()
	fmt.Println("done parsing all sites")
}
