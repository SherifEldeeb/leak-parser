package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"./leakparser"
	//_ "net/http/pprof"
)

type Record struct {
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

var ef *os.File
var err error

func c(err error) {
	if err != nil {
		log.Fatal("Error:", err)
	}
}

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
		go leakparser.HandleTextChan(textChan, recordChan)
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
