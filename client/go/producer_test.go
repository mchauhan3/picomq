package picomq

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Pico producer", func() {
	ginkgo.It("batches records and resolves durable positions", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.Header.Get("Pico-Producer-Id")).To(Equal("writer-1"))
			Expect(r.Header.Get("Pico-Producer-Epoch")).To(Equal("3"))
			Expect(r.Header.Get("Pico-Producer-Seq")).To(Equal("0"))
			data, err := body(&http.Response{Body: r.Body}, maxResponseBody)
			Expect(err).NotTo(HaveOccurred())
			Expect(binary.BigEndian.Uint32(data[1:5])).To(Equal(uint32(2)))
			w.Header().Set("Pico-Start-Seq", "5")
			w.Header().Set("Pico-Next-Seq", "7")
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		config := DefaultProducerConfig()
		config.Epoch = 3
		config.Linger = 25 * time.Millisecond
		producer := client.Stream("orders").NewProducer("writer-1", &config)
		first, err := producer.Send(context.Background(), AppendRecord{Body: []byte("a")})
		Expect(err).NotTo(HaveOccurred())
		second, err := producer.Send(context.Background(), AppendRecord{Body: []byte("b")})
		Expect(err).NotTo(HaveOccurred())
		seq, err := first.Await(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(seq).To(Equal(uint64(5)))
		seq, err = second.Await(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(seq).To(Equal(uint64(6)))
		Expect(producer.Close(context.Background())).To(Succeed())
	})
})
