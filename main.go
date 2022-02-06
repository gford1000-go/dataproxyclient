package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

type Request struct {
	Hash  string `json:"hash"`
	Token string `json:"token"`
}

type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Position int    `json:"position"`
}

type Header struct {
	Columns []Column `json:"columns"`
}

type Data struct {
	Header  Header     `json:"header"`
	Records [][]string `json:"records"`
}

type Meta struct {
	NextToken string `json:"next"`
}

type ResultSet struct {
	Meta Meta `json:"meta"`
	Data Data `json:"data"`
}

// consumePage processes the specified (hash, token) page details, retrieving the page
// and unmarshalling the return JSON results into a ResultSet.
// The duration to retrieve and unmarshal are determined, as is the number of records and
// the token for the next page (with "" signifying no further pages)
func consumePage(url, hash, token string) (string, int, time.Duration, time.Duration, error) {
	var err error

	r := Request{Hash: hash, Token: token}

	jsonData, err := json.Marshal(r)
	if err != nil {
		return "", 0, time.Duration(0), time.Duration(0), err
	}

	t1 := time.Now()

	resp, err := http.Post(url+"/page", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", 0, time.Duration(0), time.Duration(0), err
	}

	t2 := time.Now()

	var result ResultSet
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return "", 0, time.Duration(0), time.Duration(0), err
	}

	t3 := time.Now()

	return result.Meta.NextToken, len(result.Data.Records), t2.Sub(t1), t3.Sub(t2), nil
}

// consumeAllPages retrieves all the pages for the given (hash, firstToken), returning the total
// number of pages retrieved, the total number of records across these pages, and the total durations
// for retrieval and unmarshalling
func consumeAllPages(url, hash, firstToken string) (int, []int, time.Duration, time.Duration, error) {
	pageCount := 0
	recordCounts := []int{}
	totalDurationRequest := time.Duration(0)
	totalUnmarshalDuration := time.Duration(0)
	nextToken := firstToken
	for len(nextToken) > 0 {
		token, recordCount, requestDuration, unMarshalDuration, err := consumePage(url, hash, nextToken)
		if err != nil {
			return 0, nil, time.Duration(0), time.Duration(0), err
		}

		nextToken = token
		pageCount++
		recordCounts = append(recordCounts, recordCount)
		totalDurationRequest += requestDuration
		totalUnmarshalDuration += unMarshalDuration
	}

	return pageCount, recordCounts, totalDurationRequest, totalUnmarshalDuration, nil
}

// printConsumption provides a formatted output of the activity
func printConsumption(hash, firstToken string, pageCount int, recordCounts []int, totalDurationRequest, totalUnmarshalDuration time.Duration, err error) {
	fmt.Printf("Hash: %v, First Token: %v\n", hash, firstToken)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	records := 0
	for _, recordCount := range recordCounts {
		records += recordCount
	}

	fmt.Printf("  Pages: %v\n", pageCount)
	fmt.Printf("  Records: %v\n", records)
	fmt.Printf("  Duration to retrieve pages: %v\n", totalDurationRequest)
	fmt.Printf("  Duration to unmarshal pages: %v\n", totalUnmarshalDuration)
}

func main() {

	url := flag.String("url", "http://localhost:8090", "URL to dataproxy")
	hash := flag.String("hash", "", "Hash of request")
	firstToken := flag.String("token", "", "Token of first page")

	flag.Parse()

	if len(*url) == 0 || len(*hash) == 0 || len(*firstToken) == 0 {
		log.Fatal("invalid arguments")
	}

	pageCount, recordCounts, totalDurationRequest, totalUnmarshalDuration, err := consumeAllPages(*url, *hash, *firstToken)

	printConsumption(*hash, *firstToken, pageCount, recordCounts, totalDurationRequest, totalUnmarshalDuration, err)
}
