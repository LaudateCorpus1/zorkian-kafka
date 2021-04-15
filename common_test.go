package kafka

import (
	"testing"

	"github.com/op/go-logging"
	"gopkg.in/check.v1"
)

func Test(t *testing.T) {
	logging.SetLevel(logging.CRITICAL, "KafkaClient") // Suppress logs in tests.
	check.TestingT(t)
}
