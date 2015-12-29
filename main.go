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
	"syscall"
	"time"

	"github.com/cyberdelia/go-metrics-graphite"
	"github.com/gosexy/to"
	"github.com/gosexy/yaml"
	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/goagain"
)

var (
	db                      *dbConnector
	patterns                *patternStorage
	cache                   *cacheStorage
	configFileName          = flag.String("config", "/etc/moira/config.yml", "path config file")
	pidFileName             string
	logFileName             string
	listen                  string
	redisURI                string
	graphiteURI             string
	graphitePrefix          string
	graphiteInterval        int64
	retentionConfigFileName string
	totalMetricsReceived    metrics.Meter
	validMetricsReceived    metrics.Meter
	matchingMetricsReceived metrics.Meter
	matchingTimer           metrics.Timer
	savingTimer             metrics.Timer
)

func main() {
	flag.Parse()
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	log.SetPrefix(fmt.Sprintf("pid:%d ", syscall.Getpid()))

	if err := readConfig(configFileName); err != nil {
		log.Fatalf("error reading config %s: %s", *configFileName, err.Error())
	}
	logFile, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("error opening log file %s: %s", logFileName, err.Error())
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	err = ioutil.WriteFile(pidFileName, []byte(fmt.Sprint(syscall.Getpid())), 0644)
	if err != nil {
		log.Fatalf("error writing pid file %s: %s", pidFileName, err.Error())
	}

	db = newDbConnector(redisURI)

	patterns = newPatternStorage()
	cache, err = newCacheStorage()
	if err != nil {
		log.Fatalf("failed to initialize cache with config %s: %s", retentionConfigFileName, err.Error())
	}

	go patterns.refresh()

	if graphiteURI != "" {
		graphiteAddr, _ := net.ResolveTCPAddr("tcp", graphiteURI)
		go graphite.Graphite(metrics.DefaultRegistry, time.Duration(graphiteInterval)*time.Second, fmt.Sprintf("%s.cache", graphitePrefix), graphiteAddr)
	}

	totalMetricsReceived = metrics.NewRegisteredMeter("received.total", metrics.DefaultRegistry)
	validMetricsReceived = metrics.NewRegisteredMeter("received.valid", metrics.DefaultRegistry)
	matchingMetricsReceived = metrics.NewRegisteredMeter("received.matching", metrics.DefaultRegistry)
	matchingTimer = metrics.NewRegisteredTimer("time.match", metrics.DefaultRegistry)
	savingTimer = metrics.NewRegisteredTimer("time.save", metrics.DefaultRegistry)

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
	ch := make(chan *matchedMetric, 10)
	go processor(ch)
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
}

func handleConnection(conn net.Conn, ch chan *matchedMetric) {
	bufconn := bufio.NewReader(conn)

	for {
		line, err := bufconn.ReadBytes('\n')
		if err != nil {
			conn.Close()
			if err != io.EOF {
				log.Printf("read failed: %s", err.Error())
			}
			break
		}
		go func(ch chan *matchedMetric) {
			if m := processIncomingMetric(string(line)); m != nil {
				ch <- m
			}
		}(ch)
	}
}

func processor(ch chan *matchedMetric) {
	buffer := make([]*matchedMetric, 0, 10)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return
			}
			buffer = append(buffer, m)
			if len(buffer) < 10 {
				continue
			}
			break
		case <-time.After(time.Second):
			break
		}
		if len(buffer) == 0 {
			continue
		}
		timer := time.Now()
		if err := cache.savePoints(buffer); err != nil {
			log.Printf("failed to save value in cache: %s", err.Error())
		}
		savingTimer.UpdateSince(timer)
		buffer = make([]*matchedMetric, 0, 10)
	}
}
