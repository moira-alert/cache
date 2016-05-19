package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/moira-alert/cache/filter"
)

var (
	redisURI = flag.String("redis", "localhost:6379", "redis host:port")
	interval = flag.Int("interval", 60, "send each metric with specified interval in seconds")
	target   = flag.String("target", "localhost:2003", "metrics recipient host:port")
	wg       sync.WaitGroup
	stop     int32
)

func main() {
	db := filter.NewDbConnector(filter.NewRedisPool(*redisURI))
	c := db.Pool.Get()
	defer c.Close()

	log.Println("Loading metrics ...")
	keys, err := redis.Strings(c.Do("KEYS", filter.GetMetricDbKey("*")))
	if err != nil {
		log.Fatalf("Can not load metrics: %s", err)
	}
	log.Printf("Loaded %d metrics", len(keys))

	log.Printf("Generating data every %d seconds ...", *interval)
	ch := make(chan string)

	wg.Add(1)
	go func(keys []string) {
		defer wg.Done()
		for _, key := range keys {
			if atomic.LoadInt32(&stop) != 0 {
				break
			}
			wg.Add(1)
			go func(c chan string, k string, interval int) {
				defer wg.Done()
				ts := time.Now()
				for atomic.LoadInt32(&stop) == 0 {
					now := time.Now()
					if ts.Add(time.Second * time.Duration(interval)).After(now) {
						c <- k
						ts = now
					}
					time.Sleep(time.Millisecond * 100)
				}
			}(ch, strings.Split(key, ":")[1], *interval)
			time.Sleep(time.Millisecond * 100)
		}
	}(keys)

	log.Println("Sending data ...")

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go func() {
		<-interrupt
		log.Println("Stopping ...")
		atomic.StoreInt32(&stop, 1)
		wg.Wait()
		close(ch)
	}()

	conn, err := net.Dial("tcp", *target)
	if err != nil {
		log.Fatalf("Can not connect to: %s", *target)
	}
	for key := range ch {
		value := rand.Intn(10)
		if _, err := conn.Write([]byte(fmt.Sprintf("%s %d\n", key, value))); err != nil {
			log.Printf("Can not write to: %s", *target)
			break
		}
	}
	log.Println("Done.")
}
