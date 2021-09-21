package main

import (
	"sync"
	"strings"
	"fmt"
	"errors"
	"runtime"
	"flag"
	"os"
	"os/signal"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"bufio"
	"syscall"
	"io"
	log "github.com/sirupsen/logrus"
	"github.com/joonnna/workerpool"
	"time"
)

var baseUrl = "https://dataverse.no"

type Dataset struct {
	url string
	storagePath string
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var downloadDirectory string
	var linksFile string
	var poolSize int
	var scrapeLinks bool

	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&downloadDirectory, "p", "", "Directory into which store all files")
	args.StringVar(&linksFile, "links", "links.txt", "File that contains links to download.")
	args.BoolVar(&scrapeLinks, "scrape", false, "If true, scrape links and output them into 'links' file. Otherwise, assume downloading mode.")
	args.IntVar(&poolSize, "s", 10, "Number of maxiumum parallel downloads.")

	args.Parse(os.Args[1:])

	if scrapeLinks {
		if err := fetchDatasetLinks(linksFile); err != nil {
			log.Fatal(err.Error())
		}
		os.Exit(0)
	}

	if downloadDirectory == "" {
		log.Fatal("Download directory needs to be set. Exiting.")
	}

	if poolSize < 1 {
		log.Fatal("Pool size must be greater than one. Exiting.")
	}
	
	links, err := readLinksFile(linksFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	datasets, err := getDatasets(links, downloadDirectory)
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Infof("Fetching %d datasets using pool size of %d\n", len(datasets), poolSize)
	go runDownloader(datasets, poolSize)

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	os.Exit(0)
}

func fetchDatasetLinks(linksFilePath string) error {
	client := http.Client{
		Timeout: time.Duration(30 * time.Second),
	}

	resultChan := make(chan string, 1)
	errChan := make(chan error, 1)
	doneChan := make(chan struct{})

	go func() {
		rows := 10
		start := 0
		page := 1

		for {
			url := fmt.Sprintf("%s/api/search?q=*&start=%d&type=dataset", baseUrl, start)
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				errChan <-err
				continue
			}
			
			resp, err := client.Do(req)
			if err != nil {
				errChan <-err
				continue
			}

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				errChan <-err
				continue
			}

			// Array of dataset identifiers
			jsonMap := make(map[string](interface{}))
			err = json.Unmarshal(body, &jsonMap)
			if err != nil {
				errChan <-err
				continue
			}

			data := jsonMap["data"].(map[string]interface{})
			items := data["items"].([]interface{})

			for _, i := range items {
				id := i.(map[string]interface{})["global_id"]
				apiPath := "/api/access/dataset/:persistentId/?persistentId="				
				url := fmt.Sprintf("%s%s%s", baseUrl, apiPath, id)

				log.Println("Found URL", url)
				resultChan <-url
			}

			total := data["total_count"]
			start =  start + rows
			page += 1

			if int(total.(float64)) < start {
				doneChan <-struct{}{}
				return
			}
		}
	}()
	
	f, err := os.OpenFile(linksFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	
	for {
		select {
		case s := <-resultChan:
			if _, err := f.WriteString(s + "\n"); err != nil {
				log.Errorln(err.Error())
			}
		case err := <-errChan:
			log.Errorln(err.Error())

		case <-doneChan:
			log.Println("Done scraping links")
			return nil
		}
	}

	return nil
}

func readLinksFile(linksFilePath string) ([]string, error) {
	file, err := os.Open(linksFilePath)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    links := make([]string, 0)
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        links = append(links, scanner.Text())
    }

    return links, scanner.Err()
}

func runDownloader(datasets []*Dataset, poolSize int) {
	dispatcher := workerpool.NewDispatcher(uint32(poolSize))
	dispatcher.Start()
	
	wg := sync.WaitGroup{}
	counter := 1

	for _, d := range datasets {
		wg.Add(1)
		dataset := d
		c := counter
		dispatcher.Submit(func() {
			defer wg.Done()

			log.Infof("Fetching dataset %d out of %d", c, len(datasets))
			if err := dataset.downloadDataset(); err != nil {
				log.Error(err.Error())
			}
		})

		counter++
	}
}

func (d *Dataset) downloadDataset() error {
	log.Println("Fetcing dataset", d.url)

	request, err := http.NewRequest("GET", d.url, nil)
	if err != nil {
		return err
	}

	httpClient := &http.Client{
		Timeout: time.Duration(5 * time.Hour),
	}

	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	out, err := os.Create(d.storagePath + ".zip")
    if err != nil {
        return err
    }
    defer out.Close()

    _, err = io.Copy(out, response.Body)
    if err != nil {
		return err
	}

	return nil
} 

func getDatasets(links []string, storagePath string) ([]*Dataset, error) {
	if len(links) == 0 {
		return nil, errors.New("No links in list. Nothing to be done. Exiting.")
	}

	datasets := make([]*Dataset, 0, len(links))
	for _, l := range links {
		s := strings.Split(l, "dataset")
		path := strings.ReplaceAll(s[1], "/", "_")
		log.Println("path:", storagePath + "/" + path)
		ds := &Dataset{
			url: l,
			storagePath: storagePath + "/" + path,
		}


		datasets = append(datasets, ds)
	}

	return datasets, nil
}