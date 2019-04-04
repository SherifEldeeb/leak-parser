package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/go-redis/redis"
	//_ "net/http/pprof"
)

type record struct {
	// Full string
	U string
	H string
	P string
}

var (
	files     = flag.String("files", "", "Leak Filename")
	errorFile = flag.String("errorf", "errors.txt", "Errors Filename")
	pretty    = flag.Bool("pretty", false, "Pretty Print?")
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

var recordChan = make(chan string)
var textChan = make(chan string)
var counterChan = make(chan bool)
var ticker = time.Tick(time.Second)
var errorCounter = 0

var wg = &sync.WaitGroup{}
var wgNet = &sync.WaitGroup{}
var ef *os.File
var err error

func main() {
	//go http.ListenAndServe("127.0.0.1:4000", nil)

	ef, err = os.Create(*errorFile)
	if err != nil {
		panic(err)
	}
	defer ef.Close()

	flag.Parse()

	list, err := filepath.Glob(*files)
	c(err)
	if len(list) == 0 {
		c(errors.New("Nothing to process at " + *files))
	}

	wg.Add(*rcount)
	wgNet.Add(*ocount)
	for i := 0; i < *rcount; i++ {
		go handleTextChan(textChan, recordChan)
	}
	go progress(counterChan)

	for i := 0; i < *ocount; i++ {
		go func() {
			for {
				err := sendRedis(recordChan)
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

func handleTextChan(textChan chan string, recordChan chan string) {
	defer wg.Done()
	re := regexp.MustCompile("(?P<u>[^@:;,]+)@(?P<h>[^:;]+)[;:,](?P<p>.+)")
	for s := range textChan {
		// Get to work
		matches := re.FindStringSubmatch(s)
		if matches == nil || len(matches) != 4 {
			ef.Write([]byte(s + "\n"))
			continue
		}
		recordChan <- matches[1] + "\t" + matches[2] + "\t" + matches[3]
	}
}

func sendRedis(ch chan string) error {
	defer wgNet.Done()
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	for j := range ch {
		err := client.SAdd("l", j).Err()
		if err != nil {
			panic(err)
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

func ExampleNewClient() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	// Output: PONG <nil>

	err = client.Set("key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := client.Get("key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := client.Get("key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}
	// Output: key value
	// key2 does not exist

	err = client.SAdd("test").Err()
}
