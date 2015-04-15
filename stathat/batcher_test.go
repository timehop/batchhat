package stathat_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/timehop/batchhat/stathat"
)

var _ = Describe("Batcher", func() {
	Describe(".NewBatcher", func() {
		Describe("with valid parameters", func() {
			b, err := stathat.NewBatcher("ezkey", 1*time.Second)
			It("should return a batcher with no errors", func() {
				Expect(err).To(BeNil())
				Expect(b).To(BeAssignableToTypeOf(stathat.Batcher{}))
			})
		})

		Describe("with invalid parameters", func() {
			_, err := stathat.NewBatcher("ezkey", -1*time.Second)
			It("should return an error", func() {
				Expect(err).NotTo(BeNil())
				Expect(err).To(Equal(stathat.ErrInvalidFlushInterval))
			})
		})
	})

	AssertEZCall := func(action func(b stathat.Batcher), verify func(stats []*stathat.Stat)) {
		var ts *httptest.Server
		b, _ := stathat.NewBatcher("EasyEKey", 10*time.Millisecond)

		BeforeEach(func() {
			go b.Start()
		})

		AfterEach(func() {
			b.Stop()

			if ts != nil {
				ts.Close()
			}
		})

		It("should send the stat", func(done Done) {
			ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer GinkgoRecover()

				bts, err := ioutil.ReadAll(r.Body)
				defer r.Body.Close()

				Expect(err).To(BeNil())

				bs := stathat.BulkStat{}
				err = json.Unmarshal(bts, &bs)

				Expect(err).To(BeNil())

				Expect(bs.EzKey).To(Equal("EasyEKey"))

				verify(bs.Data)

				w.Write([]byte("{\"status\":200,\"msg\":\"ok\"}"))
				close(done)
			}))

			stathat.APIURL = ts.URL

			action(b)
		})
	}

	Describe("#PostEZCount", func() {
		Describe("single stat", func() {
			action := func(b stathat.Batcher) {
				b.PostEZCount("BMo", 2353)
			}

			verify := func(stats []*stathat.Stat) {
				Expect(len(stats)).To(Equal(1))

				stat := stats[0]

				Expect(stat.Stat).To(Equal("BMo"))
				Expect(stat.Count).ToNot(BeNil())
				Expect(*stat.Count).To(BeNumerically("==", 2353))
				Expect(stat.Value).To(BeNil())
				Expect(stat.Time).To(BeNumerically("~", time.Now().Unix()))
			}

			AssertEZCall(action, verify)
		})

		Describe("multiple stats", func() {
			action := func(b stathat.Batcher) {
				b.PostEZCount("BMo", 2353)
				b.PostEZCount("Bacon", 1)
				b.PostEZCount("pancakes", 200)
			}

			verify := func(stats []*stathat.Stat) {
				Expect(len(stats)).To(Equal(3))

				stat := stats[0]
				Expect(stat.Stat).To(Equal("BMo"))
				Expect(stat.Count).ToNot(BeNil())
				Expect(*stat.Count).To(BeNumerically("==", 2353))
				Expect(stat.Value).To(BeNil())
				Expect(stat.Time).To(BeNumerically("~", time.Now().Unix()))

				stat = stats[1]
				Expect(stat.Stat).To(Equal("Bacon"))
				Expect(stat.Count).ToNot(BeNil())
				Expect(*stat.Count).To(BeNumerically("==", 1))
				Expect(stat.Value).To(BeNil())
				Expect(stat.Time).To(BeNumerically("~", time.Now().Unix()))

				stat = stats[2]
				Expect(stat.Stat).To(Equal("pancakes"))
				Expect(stat.Count).ToNot(BeNil())
				Expect(*stat.Count).To(BeNumerically("==", 200))
				Expect(stat.Value).To(BeNil())
				Expect(stat.Time).To(BeNumerically("~", time.Now().Unix()))
			}

			AssertEZCall(action, verify)
		})

		Describe("zero value", func() {
			action := func(b stathat.Batcher) {
				// We need to send at least 1 non-zero value because otherwise
				// we get a timeout. Not sure why and don’t have time to investigate.
				b.PostEZCount("BMo", 1)
				b.PostEZCount("BMo", 0)
			}

			verify := func(stats []*stathat.Stat) {
				Expect(len(stats)).To(Equal(1))
				stat := stats[0]
				Expect(stat.Stat).To(Equal("BMo"))
				Expect(stat.Count).ToNot(BeNil())
				Expect(*stat.Count).To(BeNumerically("==", 1))
				Expect(stat.Value).To(BeNil())
			}

			AssertEZCall(action, verify)
		})
	})

	Describe("#PostEZCountTime", func() {
		Describe("single stat", func() {
			var now int64

			BeforeEach(func() {
				now = time.Now().Add(-24 * time.Hour).Unix()
			})

			action := func(b stathat.Batcher) {
				b.PostEZCountTime("BMo", 2353, now)
			}

			verify := func(stats []*stathat.Stat) {
				Expect(len(stats)).To(Equal(1))

				stat := stats[0]

				Expect(stat.Stat).To(Equal("BMo"))
				Expect(stat.Count).ToNot(BeNil())
				Expect(*stat.Count).To(BeNumerically("==", 2353))
				Expect(stat.Value).To(BeNil())
				Expect(stat.Time).To(BeNumerically("==", now))
			}

			AssertEZCall(action, verify)
		})

		Describe("zero value", func() {
			var now int64

			BeforeEach(func() {
				now = time.Now().Add(-24 * time.Hour).Unix()
			})

			action := func(b stathat.Batcher) {
				// We need to send at least 1 non-zero value because otherwise
				// we get a timeout. Not sure why and don’t have time to investigate.
				b.PostEZCountTime("BMo", 1, now)
				b.PostEZCountTime("BMo", 0, now)
			}

			verify := func(stats []*stathat.Stat) {
				Expect(len(stats)).To(Equal(1))
				stat := stats[0]
				Expect(stat.Stat).To(Equal("BMo"))
				Expect(stat.Count).ToNot(BeNil())
				Expect(*stat.Count).To(BeNumerically("==", 1))
				Expect(stat.Value).To(BeNil())
			}

			AssertEZCall(action, verify)
		})
	})

	Describe("#PostEZValue", func() {
		Describe("single stat", func() {
			action := func(b stathat.Batcher) {
				b.PostEZValue("BMo", 2353)
			}

			verify := func(stats []*stathat.Stat) {
				Expect(len(stats)).To(Equal(1))

				stat := stats[0]

				Expect(stat.Stat).To(Equal("BMo"))
				Expect(stat.Value).ToNot(BeNil())
				Expect(*stat.Value).To(BeNumerically("==", 2353))
				Expect(stat.Count).To(BeNil())
				Expect(stat.Time).To(BeNumerically("~", time.Now().Unix()))
			}

			AssertEZCall(action, verify)
		})

		Describe("multiple stats", func() {
			action := func(b stathat.Batcher) {
				b.PostEZValue("BMo", 2353)
				b.PostEZValue("Bacon", 1)
				b.PostEZValue("pancakes", 200)
			}

			verify := func(stats []*stathat.Stat) {
				Expect(len(stats)).To(Equal(3))

				stat := stats[0]
				Expect(stat.Stat).To(Equal("BMo"))
				Expect(stat.Value).ToNot(BeNil())
				Expect(*stat.Value).To(BeNumerically("==", 2353))
				Expect(stat.Count).To(BeNil())
				Expect(stat.Time).To(BeNumerically("~", time.Now().Unix()))

				stat = stats[1]
				Expect(stat.Stat).To(Equal("Bacon"))
				Expect(stat.Value).ToNot(BeNil())
				Expect(*stat.Value).To(BeNumerically("==", 1))
				Expect(stat.Count).To(BeNil())
				Expect(stat.Time).To(BeNumerically("~", time.Now().Unix()))

				stat = stats[2]
				Expect(stat.Stat).To(Equal("pancakes"))
				Expect(stat.Value).ToNot(BeNil())
				Expect(*stat.Value).To(BeNumerically("==", 200))
				Expect(stat.Count).To(BeNil())
				Expect(stat.Time).To(BeNumerically("~", time.Now().Unix()))
			}

			AssertEZCall(action, verify)
		})
	})

	Describe("#PostEZValueTime", func() {
		Describe("single stat", func() {
			var now int64

			BeforeEach(func() {
				now = time.Now().Add(-24 * time.Hour).Unix()
			})

			action := func(b stathat.Batcher) {
				b.PostEZValueTime("BMo", 2353, now)
			}

			verify := func(stats []*stathat.Stat) {
				Expect(len(stats)).To(Equal(1))

				stat := stats[0]

				Expect(stat.Stat).To(Equal("BMo"))
				Expect(stat.Value).ToNot(BeNil())
				Expect(*stat.Value).To(BeNumerically("==", 2353))
				Expect(stat.Count).To(BeNil())
				Expect(stat.Time).To(BeNumerically("==", now))
			}

			AssertEZCall(action, verify)
		})
	})

	Describe("posting a large number of stats", func() {
		var ts *httptest.Server
		b, _ := stathat.NewBatcher("EasyEKey", 50*time.Millisecond)

		BeforeEach(func() {
			go b.Start()
		})

		AfterEach(func() {
			b.Stop()

			if ts != nil {
				ts.Close()
			}
		})

		It("should send the stat", func(done Done) {
			iter := 0

			ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer GinkgoRecover()

				bts, err := ioutil.ReadAll(r.Body)
				defer r.Body.Close()

				Expect(err).To(BeNil())

				bs := stathat.BulkStat{}
				err = json.Unmarshal(bts, &bs)

				Expect(err).To(BeNil())

				Expect(bs.EzKey).To(Equal("EasyEKey"))

				data := bs.Data
				switch iter {
				case 0, 1:
					Expect(len(data)).To(Equal(1000))
					w.Write([]byte("{\"status\":200,\"msg\":\"ok\"}"))
				case 2:
					Expect(len(data)).To(Equal(102))
					w.Write([]byte("{\"status\":200,\"msg\":\"ok\"}"))
					close(done)
				}

				iter += 1
			}))

			stathat.APIURL = ts.URL

			for i := 0; i < 701; i++ {
				b.PostEZValue("Jake", float64(i*3))
				b.PostEZCount("the", i*5)
				b.PostEZValue("Human", float64(i*7))
			}
		})
	})
})
