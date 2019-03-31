package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"
)

type record struct {
	// Full string
	U string
	H string
	P string
}

var (
	files  = flag.String("files", "", "Leak Filename")
	pretty = flag.Bool("pretty", false, "Pretty Print?")
	// es     = flag.String("es", "http://127.0.0.1:9200", "ES Server")
	rcount   = flag.Int("rcount", 128, "Count of processing goroutines")
	ocount   = flag.Int("ocount", 8, "Count of out connections")
	logstash = flag.String("server", "127.0.0.1", "Logstash server address")
	port     = flag.String("port", "4445", "Logstash server port")
)

func c(err error) {
	if err != nil {
		log.Fatal("Error:", err)
	}
}

var recordChan = make(chan []byte)
var textChan = make(chan string)
var counterChan = make(chan bool)
var ticker = time.Tick(time.Second)
var errorCounter = 0

var wg = &sync.WaitGroup{}
var wgNet = &sync.WaitGroup{}

func main() {
	go http.ListenAndServe("127.0.0.1:4000", nil)

	flag.Parse()

	list, err := filepath.Glob(*files)
	c(err)
	if len(list) == 0 {
		c(errors.New("Nothing to process at " + *files))
	}

	wg.Add(*rcount)
	wgNet.Add(*rcount)
	for i := 0; i < *rcount; i++ {
		go handleTextChan(textChan, recordChan)
	}
	go progress(counterChan)

	for i := 0; i < *ocount; i++ {
		go func() {
			for {
				err := sendRecords(recordChan)
				if err != nil {
					errorCounter++
					if errorCounter > 3 {
						log.Println("Error threshold exceeded ... bailing out")
						os.Exit(1)
					}
					log.Println("Error connecting ... trying again one time")
					time.Sleep(time.Second)
					continue
				}
				// no errors
				os.Exit(1)
			}
		}()
	}

	for i, file := range list {
		log.Printf("file %d of %d", i+1, len(list))
		err := handle(file, textChan)
		if err != nil {
			log.Printf("Error processing: %s - %s", file, err.Error())
		}
	}

	close(textChan)
	wg.Wait()
	close(recordChan)
	wgNet.Wait()
	close(counterChan)
}

func handle(file string, textChan chan string) error {
	log.Println("Parsing:", file)
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		textChan <- scanner.Text()
	}
	return nil
	///////////////
}

func handleTextChan(textChan chan string, recordChan chan []byte) {
	defer wg.Done()
	re := regexp.MustCompile("(?P<u>[^@:;,]+)@(?P<h>[^:;]+)[;:,](?P<p>.+)")
	for s := range textChan {
		// Get to work
		matches := re.FindStringSubmatch(s)
		if matches == nil || len(matches) != 4 {
			log.Printf("error parsing:%s", s)
			continue
		}
		j, err := json.Marshal(record{
			matches[1],
			matches[2],
			matches[3],
		})
		if err != nil {
			log.Printf("error Marshaling:%v", s)
			continue
		}
		recordChan <- j
	}
}

func sendRecords(ch chan []byte) error {
	defer wgNet.Done()
	// TCP
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", *logstash, *port))
	if err != nil {
		return err
	}
	defer conn.Close()
	for j := range ch {
		_, err = conn.Write(j)
		if err != nil {
			return err
		}
		_, err = conn.Write([]byte{'\n'})
		if err != nil {
			return err
		}
		counterChan <- true
		//fmt.Print(".")
	}
	return nil
}

func progress(counter chan bool) {
	var c uint64

	go func() {
		for <-counter {
			c++
		}
	}()

	for t := range ticker {
		_ = t
		log.Printf("Count: %d", c)
	}
}
