package main

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws/session"
	transportfirehose "github.com/fujiwara/go-http-transport-firehose"
)

func main() {
	sess := session.Must(session.NewSession())

	client := http.Client{
		Transport: transportfirehose.New(sess, "http-out"),
	}

	req, _ := http.NewRequest("POST", "http://example.com/hello", bytes.NewReader([]byte("hello\n")))
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.Status)
}
