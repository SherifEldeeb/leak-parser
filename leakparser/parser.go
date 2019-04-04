package leakparser

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"sync"
	"time"
)

type Record struct {
	// Full string
	U string
	H string
	P string
}

var wg = &sync.WaitGroup{}
var wgNet = &sync.WaitGroup{}
var ef *os.File
var err error
var recordChan = make(chan []byte)
var textChan = make(chan string)
var counterChan = make(chan bool)
var ticker = time.Tick(time.Second)
var errorCounter = 0

func Handle(file string, textChan chan string) error {
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

func HandleTextChan(textChan chan string, recordChan chan []byte) {
	defer wg.Done()
	re := regexp.MustCompile("(?P<u>[^@:;,]+)@(?P<h>[^:;]+)[;:,](?P<p>.+)")
	for s := range textChan {
		// Get to work
		matches := re.FindStringSubmatch(s)
		if matches == nil || len(matches) != 4 {
			ef.Write([]byte(s + "\n"))
			continue
		}
		j, err := json.Marshal(Record{
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

func SendRecords(ch chan []byte, logstash string, port string) error {
	defer wgNet.Done()
	// TCP
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", logstash, port))
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

func Progress(counter chan bool) {
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
