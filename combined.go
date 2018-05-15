package jaeger

import (
	"strconv"
	"strings"
	//"log"

	opentracing "github.com/opentracing/opentracing-go"
)

type Propagator struct {
	uber *textMapPropagator
}

func NewCombinedB3HTTPHeaderPropagator() Propagator {
	return Propagator{
		uber: newHTTPHeaderPropagator(getDefaultHeadersConfig(), *NewNullMetrics()),
	}
}

// Inject conforms to the Injector interface for decoding Zipkin HTTP B3 headers
func (p Propagator) Inject(
	sc SpanContext,
	abstractCarrier interface{},
) error {
	textMapWriter, ok := abstractCarrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	p.uber.Inject(sc, abstractCarrier)

	// TODO this needs to change to support 128bit IDs
	textMapWriter.Set("x-b3-traceid", strconv.FormatUint(sc.TraceID().Low, 16))
	if sc.ParentID() != 0 {
		textMapWriter.Set("x-b3-parentspanid", strconv.FormatUint(uint64(sc.ParentID()), 16))
	}
	textMapWriter.Set("x-b3-spanid", strconv.FormatUint(uint64(sc.SpanID()), 16))
	if sc.IsSampled() {
		textMapWriter.Set("x-b3-sampled", "1")
	} else {
		textMapWriter.Set("x-b3-sampled", "0")
	}
	return nil
}

// Extract conforms to the Extractor interface for encoding Zipkin HTTP B3 headers
func (p Propagator) Extract(abstractCarrier interface{}) (SpanContext, error) {
	textMapReader, ok := abstractCarrier.(opentracing.TextMapReader)
	if !ok {
		return SpanContext{}, opentracing.ErrInvalidCarrier
	}

	if sc, err := p.uber.Extract(abstractCarrier); err == nil {
		//log.Printf("Extracted from uber headers\n%+v\n", sc)
		return sc, err
	}

	var traceID uint64
	var spanID uint64
	var parentID uint64
	sampled := false
	err := textMapReader.ForeachKey(func(rawKey, value string) error {
		key := strings.ToLower(rawKey) // TODO not necessary for plain TextMap
		var err error
		if key == "x-b3-traceid" {
			traceID, err = strconv.ParseUint(value, 16, 64)
		} else if key == "x-b3-parentspanid" {
			parentID, err = strconv.ParseUint(value, 16, 64)
		} else if key == "x-b3-spanid" {
			spanID, err = strconv.ParseUint(value, 16, 64)
		} else if key == "x-b3-sampled" && value == "1" {
			sampled = true
		}
		return err
	})

	if err != nil {
		//log.Printf("ERR on B3 headers %+v\n", err)
		return SpanContext{}, err
	}
	if traceID == 0 {
		//log.Printf("traceID==0 on B3 headers %+v\n", err)
		return SpanContext{}, opentracing.ErrSpanContextNotFound
	}
	return NewSpanContext(
		TraceID{Low: traceID},
		SpanID(spanID),
		SpanID(parentID),
		sampled, nil), nil
}
