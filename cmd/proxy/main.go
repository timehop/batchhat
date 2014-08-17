package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

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
