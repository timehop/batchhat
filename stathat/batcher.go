package stathat

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/timehop/golog/log"
)

const (
	logID = "Stathat Batcher"
)

var (
	APIURL = "http://api.stathat.com/ez"

	ErrCouldNotQueueStat    = errors.New("could not queue up stat")
	ErrInvalidFlushInterval = errors.New("flush interval invalid")
)

type Flusher interface {
	Flush(ezkey string, stats []Stat)
}

type Stat struct {
	Stat  string  `json:"stat"`
	Count float64 `json:"count,omitempty"`
	Value float64 `json:"value,omitempty"`
	Time  int64   `json:"t"`
}

type BulkStat struct {
	EzKey string  `json:"ezkey"`
	Data  []*Stat `json:"data"`
}

type Batcher struct {
	Stats         chan Stat
	stop          chan interface{}
	ezKey         string
	flushInterval time.Duration
}

func NewBatcher(ezKey string, d time.Duration) (Batcher, error) {
	if d < 0 {
		return Batcher{}, ErrInvalidFlushInterval
	}

	c := make(chan Stat, 10000)
	st := make(chan interface{})

	return Batcher{ezKey: ezKey, flushInterval: d, stop: st, Stats: c}, nil
}

func (b Batcher) PostEZCount(statName string, count int) error {
	return b.PostEZCountTime(statName, count, time.Now().Unix())
}

func (b Batcher) PostEZCountTime(statName string, count int, timestamp int64) error {
	s := Stat{
		Stat:  statName,
		Count: float64(count),
		Time:  timestamp,
	}

	return b.record(s)
}

func (b Batcher) PostEZValue(statName string, value float64) error {
	return b.PostEZValueTime(statName, value, time.Now().Unix())
}

func (b Batcher) PostEZValueTime(statName string, value float64, timestamp int64) error {
	s := Stat{
		Stat:  statName,
		Value: value,
		Time:  timestamp,
	}

	return b.record(s)
}

func (b Batcher) Stop() error {
	b.stop <- true
	return nil
}

func (b Batcher) record(s Stat) error {
	b.Stats <- s
	return nil
}

func (b Batcher) Start() {
	ss := []*Stat{}
	t := time.Tick(b.flushInterval)

	for {
		select {
		case s := <-b.Stats:
			ss = append(ss, &s)
		case <-t:
			go b.flush(ss)
			ss = []*Stat{}
		case <-b.stop:
			break
		}
	}
}

func (b Batcher) flush(stats []*Stat) {
	if len(stats) == 0 {
		return
	}

	chunks := len(stats) / 1000
	for i := 0; i <= chunks; i++ {
		start := i * 1000
		end := start + 1000

		if end > len(stats)-1 {
			end = len(stats)
		}

		j, err := json.Marshal(BulkStat{EzKey: b.ezKey, Data: stats[start:end]})
		if err != nil {
			log.Warn(logID, "couldn't marshal bulk data", "error", err.Error())
			return
		}

		req, err := http.NewRequest("POST", APIURL, bytes.NewReader(j))
		if err != nil {
			log.Warn(logID, "couldn't make request", "error", err.Error())
			return
		}

		req.Header.Add("Content-Type", "application/json")

		retries := 2
		for i := 0; i < retries; i++ {
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Warn(logID, "error posting data to stathat", "error", err.Error())
				continue
			}

			b, _ := ioutil.ReadAll(resp.Body)
			defer resp.Body.Close()

			log.Debug(logID, "Flushed", "status", resp.Status, "resp", string(b))
			break
		}
	}
}
