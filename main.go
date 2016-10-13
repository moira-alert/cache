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
)

var (
	configFileName          = flag.String("config", "/etc/moira/config.yml", "path config file")
	logParseErrors          = flag.Bool("logParseErrors", false, "enable logging metric parse errors")
	printVersion            = flag.Bool("version", false, "Print version and exit")
	pidFileName             string
	logFileName             string
	listen                  string
	redisURI                string
	graphiteURI             string
	graphitePrefix          string
	graphiteInterval        int64
	retentionConfigFileName string
	dbID                    int
	db                      *filter.DbConnector
	cache                   *filter.CacheStorage
	patterns                *filter.PatternStorage

	version = "undefined"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	if *printVersion {
		fmt.Printf("Moira Cache version: %s\n", version)
		os.Exit(0)
	}

	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	filter.LogParseErrors = *logParseErrors
	log.SetPrefix(fmt.Sprintf("pid:%d ", syscall.Getpid()))

	if err := readConfig(configFileName); err != nil {
		log.Fatalf("error reading config [%s]: %s", *configFileName, err.Error())
	}
	logFile, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("error opening log file [%s]: %s", logFileName, err.Error())
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	err = ioutil.WriteFile(pidFileName, []byte(fmt.Sprint(syscall.Getpid())), 0644)
	if err != nil {
		log.Fatalf("error writing pid file [%s]: %s", pidFileName, err.Error())
	}

	retentionConfigFile, err := os.Open(retentionConfigFileName)
	if err != nil {
		log.Fatalf("error open retentions file [%s]: %s", pidFileName, err.Error())
	}

	filter.InitGraphiteMetrics()

	db = filter.NewDbConnector(filter.NewRedisPool(redisURI, dbID))
	patterns = filter.NewPatternStorage()
	if err = patterns.DoRefresh(db); err != nil {
		log.Fatalf("failed to refresh pattern storage: %s", err.Error())
	}
	cache, err = filter.NewCacheStorage(bufio.NewScanner(retentionConfigFile))
	if err != nil {
		log.Fatalf("failed to initialize cache with config [%s]: %s", retentionConfigFileName, err.Error())
	}

	terminate := make(chan bool)

	var wg sync.WaitGroup

	wg.Add(1)
	go patterns.Refresh(db, terminate, &wg)

	wg.Add(1)
	go heartbeat(db, terminate, &wg)

	if graphiteURI != "" {
		graphiteAddr, _ := net.ResolveTCPAddr("tcp", graphiteURI)
		go graphite.Graphite(metrics.DefaultRegistry, time.Duration(graphiteInterval)*time.Second, fmt.Sprintf("%s.cache", graphitePrefix), graphiteAddr)
	}

	l, err := goagain.Listener()
	if err != nil {
		l, err = net.Listen("tcp", listen)
		if err != nil {
			log.Fatalf("failed to listen on [%s]: %s", listen, err.Error())
		}
		log.Printf("listening on %s", listen)
		wg.Add(1)
		go serve(l, terminate, &wg)

	} else {
		log.Printf("resuming listening on %s", listen)

		wg.Add(1)
		go serve(l, terminate, &wg)

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
	wg.Wait()
	log.Printf("shutdown complete")
}

func readConfig(configFileName *string) error {
	file, err := yaml.Open(*configFileName)
	if err != nil {
		return fmt.Errorf("Can't read config file [%s]: %s", *configFileName, err.Error())
	}
	pidFileName = to.String(file.Get("cache", "pid"))
	logFileName = to.String(file.Get("cache", "log_file"))
	listen = to.String(file.Get("cache", "listen"))
	retentionConfigFileName = to.String(file.Get("cache", "retention-config"))
	redisURI = fmt.Sprintf("%s:%s", to.String(file.Get("redis", "host")), to.String(file.Get("redis", "port")))
	graphiteURI = to.String(file.Get("graphite", "uri"))
	graphitePrefix = to.String(file.Get("graphite", "prefix"))
	graphiteInterval = to.Int64(file.Get("graphite", "interval"))
	dbID = int(to.Int64(file.Get("redis", "dbid")))
	return nil
}

func serve(l net.Listener, terminate chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	metricsChan := make(chan *filter.MatchedMetric, 10)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cache.ProcessMatchedMetrics(metricsChan, func(buffer map[string]*filter.MatchedMetric) {
			if err := cache.SavePoints(buffer, db); err != nil {
				log.Printf("failed to save value in cache: %s", err.Error())
			}
		})
	}()
	go func() {
		for {
			select {
			case <-terminate:
				return
			case <-time.After(time.Second):
				filter.UpdateProcessingMetrics()
			}
		}
	}()
	var handleWG sync.WaitGroup
	for {
		conn, err := l.Accept()
		if err != nil {
			if goagain.IsErrClosing(err) {
				log.Println("Listener closed")
				close(terminate)
				break
			}
			log.Printf("failed to accept connection: %s", err.Error())
			continue
		}
		handleWG.Add(1)
		go func(conn net.Conn, ch chan *filter.MatchedMetric) {
			defer handleWG.Done()
			handleConnection(conn, ch, terminate, &handleWG)
		}(conn, metricsChan)
	}
	handleWG.Wait()
	close(metricsChan)
}

func handleConnection(conn net.Conn, ch chan *filter.MatchedMetric, terminate chan bool, wg *sync.WaitGroup) {
	bufconn := bufio.NewReader(conn)

	go func(conn net.Conn) {
		<-terminate
		conn.Close()
	}(conn)

	for {
		lineBytes, err := bufconn.ReadBytes('\n')
		if err != nil {
			conn.Close()
			if err != io.EOF {
				log.Printf("read failed: %s", err)
			}
			break
		}
		lineBytes = lineBytes[:len(lineBytes)-1]
		wg.Add(1)
		go func(ch chan *filter.MatchedMetric) {
			defer wg.Done()
			if m := patterns.ProcessIncomingMetric(lineBytes); m != nil {
				ch <- m
			}
		}(ch)
	}
}
