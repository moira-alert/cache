package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/cyberdelia/go-metrics-graphite"
	"github.com/gosexy/to"
	"github.com/gosexy/yaml"
	"github.com/moira-alert/cache/filter"
	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/goagain"
	"github.com/garyburd/redigo/redis"
)

var (
	configFileName          = flag.String("config", "/etc/moira/config.yml", "path config file")
	pidFileName             string
	logFileName             string
	listen                  string
	redisURI                string
	graphiteURI             string
	graphitePrefix          string
	graphiteInterval        int64
	retentionConfigFileName string
	db                      *filter.DbConnector
	cache                   *filter.CacheStorage
	patterns                *filter.PatternStorage
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	log.SetPrefix(fmt.Sprintf("pid:%d ", syscall.Getpid()))

	if err := readConfig(configFileName); err != nil {
		log.Fatalf("error reading config %s: %s", *configFileName, err)
	}
	logFile, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("error opening log file %s: %s", logFileName, err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	err = ioutil.WriteFile(pidFileName, []byte(fmt.Sprint(syscall.Getpid())), 0644)
	if err != nil {
		log.Fatalf("error writing pid file %s: %s", pidFileName, err)
	}

	retentionConfigFile, err := os.Open(retentionConfigFileName)
	if err != nil {
		log.Fatalf("error open retentions file %s: %s", pidFileName, err)
	}

	filter.InitGraphiteMetrics()

	db = filter.NewDbConnector(newRedisPool(redisURI))
	patterns = filter.NewPatternStorage()
	cache, err = filter.NewCacheStorage(retentionConfigFile)
	if err != nil {
		log.Fatalf("failed to initialize cache with config %s: %s", retentionConfigFileName, err.Error())
	}

	go patterns.Refresh(db)

	if graphiteURI != "" {
		graphiteAddr, _ := net.ResolveTCPAddr("tcp", graphiteURI)
		go graphite.Graphite(metrics.DefaultRegistry, time.Duration(graphiteInterval)*time.Second, fmt.Sprintf("%s.cache", graphitePrefix), graphiteAddr)
	}

	l, err := goagain.Listener()
	if err != nil {
		l, err = net.Listen("tcp", listen)
		if err != nil {
			log.Fatalf("failed to listen on %s: %s", listen, err.Error())
		}
		log.Printf("listening on %s", listen)

		go serve(l)

	} else {
		log.Printf("resuming listening on %s", listen)

		go serve(l)

		if err := goagain.Kill(); err != nil {
			log.Fatalf("failed to kill parent process: %s", err.Error())
		}
	}

	if _, err := goagain.Wait(l); err != nil {
		log.Fatalf("failed to block main goroutine: %s", err.Error())
	}

	log.Printf("shutting down")
	if err := l.Close(); err != nil {
		log.Fatalf("failed to stop listening: %s", err.Error())
	}
	time.Sleep(time.Second)
	log.Printf("shutdown complete")
}

func readConfig(configFileName *string) error {
	file, err := yaml.Open(*configFileName)
	if err != nil {
		return fmt.Errorf("Can't read config file %s: %s", *configFileName, err.Error())
	}
	pidFileName = to.String(file.Get("cache", "pid"))
	logFileName = to.String(file.Get("cache", "log_file"))
	listen = to.String(file.Get("cache", "listen"))
	retentionConfigFileName = to.String(file.Get("cache", "retention-config"))
	redisURI = fmt.Sprintf("%s:%s", to.String(file.Get("redis", "host")), to.String(file.Get("redis", "port")))
	graphiteURI = to.String(file.Get("graphite", "uri"))
	graphitePrefix = to.String(file.Get("graphite", "prefix"))
	graphiteInterval = to.Int64(file.Get("graphite", "interval"))
	return nil
}

func serve(l net.Listener) {
	var wg sync.WaitGroup
	ch := make(chan *filter.MatchedMetric, 10)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cache.Save(ch, func(buffer []*filter.MatchedMetric) {
			if err := cache.SavePoints(buffer, db); err != nil {
				log.Printf("failed to save value in cache: %s", err)
			}
		})
	}()
	go func() {
		for {
			filter.UpdateProcessingMetrics()
			time.Sleep(time.Second)
		}
	}()
	for {
		conn, err := l.Accept()
		if err != nil {
			if goagain.IsErrClosing(err) {
				break
			}
			log.Printf("failed to accept connection: %s", err.Error())
			continue
		}

		go handleConnection(conn, ch)
	}
	close(ch)
	wg.Wait()
}

func handleConnection(conn net.Conn, ch chan *filter.MatchedMetric) {
	bufconn := bufio.NewReader(conn)

	for {
		lineBytes, err := bufconn.ReadBytes('\n')
		if err != nil {
			conn.Close()
			if err != io.EOF {
				log.Printf("read failed: %s", err)
			}
			break
		}
		go func(ch chan *filter.MatchedMetric) {
			if m := patterns.ProcessIncomingMetric(lineBytes); m != nil {
				ch <- m
			}
		}(ch)
	}
}

func newRedisPool(redisURI string, dbID ...int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisURI)
			if err != nil {
				return nil, err
			}
			if len(dbID) > 0 {
				c.Do("SELECT", dbID[0])
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}