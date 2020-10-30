package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/fujiwara/ridge"
)

const (
	commonAttrHeaderName = "X-Amz-Firehose-Common-Attributes"
	requestIDHeaderName  = "X-Amz-Firehose-Request-Id"
	accessKeyHeaderName  = "X-Amz-Firehose-Access-Key"
)

var AccessKey string

func init() {
	AccessKey = os.Getenv("ACCESS_KEY")
}

// FirehoseCommonAttributes represents common attributes (metadata).
// https://docs.aws.amazon.com/ja_jp/firehose/latest/dev/httpdeliveryrequestresponse.html#requestformat
type FirehoseCommonAttributes struct {
	CommonAttributes map[string]string `json:"commonAttributes"`
}

// RequestBody represents request body.
type RequestBody struct {
	RequestID string   `json:"requestId,omitempty"`
	Timestamp int64    `json:"timestamp,omitempty"`
	Records   []Record `json:"records,omitempty"`
}

// Record represents records in request body.
type Record struct {
	Data []byte `json:"data"`
}

// ResponseBody represents response body.
// https://docs.aws.amazon.com/ja_jp/firehose/latest/dev/httpdeliveryrequestresponse.html#responseformat
type ResponseBody struct {
	RequestID    string `json:"requestId,omitempty"`
	Timestamp    int64  `json:"timestamp,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

func main() {
	var mux = http.NewServeMux()
	mux.HandleFunc("/", handleRoot)
	ridge.Run(":8080", "/", mux)
}

func parseRequest(r *http.Request) (*RequestBody, error) {
	accessKey := r.Header.Get(accessKeyHeaderName)
	if accessKey == AccessKey {
		return nil, errors.New("[error] invalid access key")
	}

	var body RequestBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("[error] failed to decode request body: %s", err)
	}
	return &body, nil
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("content-type", "application/json")
	respBody := ResponseBody{
		RequestID: r.Header.Get(requestIDHeaderName),
	}
	defer func() {
		respBody.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
		if e := respBody.ErrorMessage; e != "" {
			log.Printf("[error] error:%s", e)
		}
		json.NewEncoder(w).Encode(respBody)
	}()
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		respBody.ErrorMessage = "POST method required"
		return
	}

	reqBody, err := parseRequest(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		respBody.ErrorMessage = err.Error()
		return
	}
	for _, record := range reqBody.Records {
		req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(record.Data)))
		if err != nil {
			log.Println("[warn] failed to read request. skip", err)
			continue
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			respBody.ErrorMessage = err.Error()
			return
		}
		if resp.StatusCode >= 400 {
			respBody.ErrorMessage = fmt.Sprintf("failed to request to %s with status %d", req.URL, resp.StatusCode)
			return
		}
		log.Printf("[info] succeeded request to %s with status %d", req.URL, resp.StatusCode)
	}
}
