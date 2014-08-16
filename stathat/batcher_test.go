package stathat_test

import (
	"fmt"
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

	Describe("#PostEZCount", func() {
		b, _ := stathat.NewBatcher("ezkey", 10*time.Millisecond)
		It("should send the stat", func(done Done) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, "Hello, client")

				close(done)

				// ts.Close()
				b.Stop()
			}))

			stathat.APIURL = ts.URL
			fmt.Println(ts.URL)

			go b.Start()

			b.PostEZCount("yo", 3)
			b.PostEZCount("yo", 3)
			b.PostEZCount("yo", 3)
			b.PostEZCount("yo", 3)
			b.PostEZCount("yo", 3)
			b.PostEZCount("yo", 3)
			b.PostEZCount("yo", 3)
			b.PostEZCount("yo", 3)
			b.PostEZCount("yo", 3)
			b.PostEZCount("yo", 3)
			b.PostEZCount("yo", 3)
			b.PostEZCount("yo", 3)
		})
	})
})
