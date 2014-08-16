package stathat_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestStathat(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Stathat Suite")
}
