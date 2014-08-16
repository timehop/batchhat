package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

var (
	ezkeys map[string]chan stat
)

type stat struct {
	Stat  string  `json:"stat"`
	Count float64 `json:"count,omitempty"`
	Value float64 `json:"value,omitempty"`
	Time  int64   `json:"t"`
}

type ezkeyStat struct {
	ezkey string
	stat  stat
}

type bulkStat struct {
	EzKey string  `json:"ezkey"`
	Data  []*stat `json:"data"`
}

type flusher struct {
	ezkey string
	stats []stat
}

type nfloat64 struct {
	value   float64
	present bool
}

func errorResp(w http.ResponseWriter, err error) {
	fmt.Println("errored", err.Error())
	fmt.Fprintf(w, "{\"status\":500,\"msg\":\"%v\"}", err.Error())
}

func singleArg(r *http.Request, key string) (string, error) {
	vals := r.Form[key]

	if len(vals) == 0 {
		return "", nil
	} else if len(vals) > 1 {
		return "", fmt.Errorf("too many values for %v", key)
	}

	return vals[0], nil
}

func requiredParam(r *http.Request, key string) (string, error) {
	val, err := singleArg(r, key)

	if err != nil {
		return "", err
	}

	if val == "" {
		return "", fmt.Errorf("no %v specified", key)
	}

	return val, nil
}

func floatParam(r *http.Request, key string) (nfloat64, error) {
	var param nfloat64
	val, err := singleArg(r, key)

	// Bubble up the error
	if err != nil {
		return param, err
	}

	// If empty, return empty nfloat64
	if val == "" {
		return param, nil
	}

	nf, err := strconv.ParseFloat(val, 64)

	// Bubble up the error
	if err != nil {
		return param, fmt.Errorf("invalid number")
	}

	return nfloat64{value: nf, present: true}, nil
}

func ezHandler(w http.ResponseWriter, r *http.Request, es chan ezkeyStat) {
	r.ParseForm()

	ez, err := requiredParam(r, "ezkey")
	if err != nil {
		errorResp(w, err)
		return
	}

	s, err := requiredParam(r, "stat")
	if err != nil {
		errorResp(w, err)
		return
	}

	c, err := floatParam(r, "count")
	if err != nil {
		errorResp(w, err)
		return
	}

	v, err := floatParam(r, "value")
	if err != nil {
		errorResp(w, err)
		return
	}

	t, err := floatParam(r, "t")
	if err != nil {
		errorResp(w, err)
		return
	}

	var ts int64
	if t.present {
		ts = int64(t.value)
	} else {
		ts = time.Now().Unix()
	}

	var st stat

	if c.present {
		st = stat{Stat: s, Time: ts, Count: c.value}
	} else if v.present {
		st = stat{Stat: s, Time: ts, Value: v.value}
	} else {
		errorResp(w, fmt.Errorf("missing count or value"))
		return
	}

	go func() { es <- ezkeyStat{ezkey: ez, stat: st} }()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"status\":200,\"msg\":\"ok\"}")
}

func flush(ezs map[string][]*stat) {
	for ezkey, stats := range ezs {
		if len(stats) == 0 {
			continue
		}

		chunks := len(stats) / 1000
		for i := 0; i <= chunks; i++ {
			start := i * 1000
			end := start + 1000

			if end > len(stats)-1 {
				end = len(stats) - 1
			}

			log.Println("stats len start end", len(stats), start, end)

			j, err := json.Marshal(bulkStat{EzKey: ezkey, Data: stats[start:end]})
			if err != nil {
				fmt.Println("Couldn't marshal bulk data", err.Error())
				return
			}

			req, err := http.NewRequest("POST", "http://api.stathat.com/ez", bytes.NewReader(j))
			if err != nil {
				fmt.Println("Couldn't make request", err.Error())
				return
			}

			req.Header.Add("Content-Type", "application/json")

			retries := 2
			for i := 0; i < retries; i++ {
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					fmt.Println("error posting data", err.Error())
					continue
				}

				b, _ := ioutil.ReadAll(resp.Body)
				defer resp.Body.Close()

				log.Println("Flushed", resp.Status, string(b))
				break
			}
		}
	}
}

func collect(es chan ezkeyStat) {
	ezss := map[string][]*stat{}

	t := time.Tick(15 * time.Second)
	for {
		select {
		case ezs := <-es:
			ezss[ezs.ezkey] = append(ezss[ezs.ezkey], &ezs.stat)
		case <-t:
			go flush(ezss)
			ezss = map[string][]*stat{}
		}
	}
}

func main() {
	es := make(chan ezkeyStat)

	go collect(es)

	http.HandleFunc("/ez", func(w http.ResponseWriter, r *http.Request) {
		ezHandler(w, r, es)
	})

	err := http.ListenAndServe(":"+os.Getenv("PORT"), nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
