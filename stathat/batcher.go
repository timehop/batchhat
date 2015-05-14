package stathat

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math"
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
	ErrNaN                  = errors.New("value is not a number (NaN)")
)

type Stat struct {
	Stat  string   `json:"stat"`
	Count *float64 `json:"count,omitempty"`
	Value *float64 `json:"value,omitempty"`
	Time  int64    `json:"t"`
}

type BulkStat struct {
	EzKey string  `json:"ezkey"`
	Data  []*Stat `json:"data"`
}

type Batcher struct {
	Stats         chan Stat
	stop          chan interface{}
	EZKey         string
	flushInterval time.Duration
}

func NewBatcher(ezKey string, d time.Duration) (Batcher, error) {
	if d < 0 {
		return Batcher{}, ErrInvalidFlushInterval
	}

	c := make(chan Stat, 10000)
	st := make(chan interface{})

	return Batcher{EZKey: ezKey, flushInterval: d, stop: st, Stats: c}, nil
}

// PostEZCount enqueues a given integer value to be added to a StatHat counter stat. 0 values will
// be dropped.
func (b Batcher) PostEZCount(statName string, count int) error {
	// If a caller is sending a 0, they probably intend for it to be a no-op — to essentially
	// increment the key by 0, i.e. by nothing. But in fact if we send a 0 to StatHat using their
	// `PostEZCount` method, they actually increment the key by 1, for some crazy reason. This is
	// bad, it violates the principle of least surprise. So we’ll just throw the value away and
	// do nothing as I’d think the caller would expect. This would also be good even if StatHat
	// did the intuitive thing, because this saves unnecessary resources and time in sending a
	// noop request.
	if count == 0 {
		return nil
	}

	return b.PostEZCountTime(statName, count, time.Now().Unix())
}

// PostEZCount enqueues a given integer value to be added to a StatHat counter stat with a specific
// timestamp. 0 values will be dropped.
func (b Batcher) PostEZCountTime(statName string, count int, timestamp int64) error {
	// If a caller is sending a 0, they probably intend for it to be a no-op — to essentially
	// increment the key by 0, i.e. by nothing. But in fact if we send a 0 to StatHat using their
	// `PostEZCount` method, they actually increment the key by 1, for some crazy reason. This is
	// bad, it violates the principle of least surprise. So we’ll just throw the value away and
	// do nothing as I’d think the caller would expect. This would also be good even if StatHat
	// did the intuitive thing, because this saves unnecessary resources and time in sending a
	// noop request.
	if count == 0 {
		return nil
	}

	// Neither count nor timestamp can ever be NaN due to their types (not float64.)
	c := float64(count)
	s := Stat{
		Stat:  statName,
		Count: &c,
		Time:  timestamp,
	}

	return b.record(s)
}

func (b Batcher) PostEZValue(statName string, value float64) error {
	return b.PostEZValueTime(statName, value, time.Now().Unix())
}

func (b Batcher) PostEZValueTime(statName string, value float64, timestamp int64) error {
	if math.IsNaN(value) {
		return ErrNaN
	}

	s := Stat{
		Stat:  statName,
		Value: &value,
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
			// Only do any work if EZKey is set
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

	if b.EZKey == "" {
		log.Warn(logID, "Skipping flush. ez key not set.", "stats", len(stats))
		return
	}

	for chunk := range chunks(stats) {
		j, err := json.Marshal(BulkStat{EzKey: b.EZKey, Data: chunk})
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

		send(req, 2)
	}
}

func chunks(stats []*Stat) chan []*Stat {
	c := make(chan []*Stat)
	go func() {
		chunks := len(stats) / 1000
		for i := 0; i <= chunks; i++ {

			start := i * 1000
			end := start + 1000

			if end > len(stats)-1 {
				end = len(stats)
			}

			c <- stats[start:end]
		}

		close(c)
	}()

	return c
}

func send(req *http.Request, retries int) {
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
