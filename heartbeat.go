package main

import (
	"log"
	"sync"
	"time"

	"github.com/moira-alert/cache/filter"
)

func heartbeat(db *filter.DbConnector, terminate chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	count := filter.TotalMetricsReceived.Count()
	for {
		select {
		case <-terminate:
			return
		case <-time.After(time.Second * 5):
			newCount := filter.TotalMetricsReceived.Count()
			if newCount != count {
				if err := db.UpdateMetricsHeartbeat(); err != nil {
					log.Printf("Save state failed: %s", err.Error())
				} else {
					count = newCount
				}
			}
		}
	}
}
