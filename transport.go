package transportfirehose

import (
	"log"
	"net/http"
	"net/http/httputil"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
)

var Debug = false

type RoundTripper struct {
	svc        *firehose.Firehose
	streamName string
}

func New(sess *session.Session, streamName string) *RoundTripper {
	return &RoundTripper{
		svc:        firehose.New(sess),
		streamName: streamName,
	}
}

func (t *RoundTripper) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	resp = &http.Response{
		StatusCode: http.StatusAccepted,
		Proto:      req.Proto,
		ProtoMajor: req.ProtoMajor,
		ProtoMinor: req.ProtoMinor,
		Header:     http.Header{"content-type": []string{"text/plain"}},
	}
	defer func() {
		if err != nil {
			resp.StatusCode = http.StatusInternalServerError
		}
		resp.Status = http.StatusText(resp.StatusCode)
	}()

	dump, err := httputil.DumpRequestOut(req, true)
	if err != nil {
		return
	}
	if Debug {
		log.Println("[debug] request dump", string(dump))
	}

	out, err := t.svc.PutRecord(&firehose.PutRecordInput{
		DeliveryStreamName: &t.streamName,
		Record:             &firehose.Record{Data: dump},
	})
	if err != nil {
		return
	}
	if Debug {
		log.Println("[debug] put record output", out)
	}
	return
}
