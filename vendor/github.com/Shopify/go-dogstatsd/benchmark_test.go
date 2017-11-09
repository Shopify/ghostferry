package dogstatsd

import (
	old "github.com/ooyala/go-dogstatsd"
	"testing"
)

var myTags = []string{"a", "b", "c"}

func BenchmarkNewSet(b *testing.B) {
	c, _ := New("localhost:9421", nil) // probably unused
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set("test", "item", myTags, 0.9999)
	}
}

func BenchmarkOldSet(b *testing.B) {
	c, _ := old.New("localhost:9421") // probably unused
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set("test", "item", myTags, 0.9999)
	}
}

func BenchmarkNewGauge(b *testing.B) {
	c, _ := New("localhost:9421", nil) // probably unused
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Gauge("test", 1.23412, myTags, 0.9999)
	}
}

func BenchmarkOldGauge(b *testing.B) {
	c, _ := old.New("localhost:9421") // probably unused
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Gauge("test", 1.23412, myTags, 0.9999)
	}
}
