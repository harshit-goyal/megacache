//go:build ignore

package main

import (
	"net/http"
	"time"

	megacache "github.com/harshit-goyal/megacache/sdk/go"
)

func main() {
	client, _ := megacache.New(megacache.Options{})
	noStalePolicy := megacache.CachePolicy{TTL: time.Minute}.WithStale(0)
	handler := megacache.TraceparentMiddleware(client, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) },
	))
	_ = noStalePolicy
	_ = handler
}
