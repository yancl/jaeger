// Copyright (c) 2018 The Jaeger Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"time"

	"encoding/binary"
	"github.com/gogo/protobuf/proto"
	"strings"

	"github.com/census-instrumentation/opencensus-proto/gen-go/traceproto"
	"github.com/jaegertracing/jaeger/model"
	"github.com/yancl/hunter-proto/gen-go/dumpproto"
)

const (
	SERVICE_NAME_KEY = "service_name"
	KIND_KEY         = "kind"
	REMOTE_KIND_KEY  = "remote_kind"
	REMOTE_ADDR_KEY  = "remote_addr"
	HOST_NAME_KEY    = "hostname"
	QUERY_KEY        = "query"
)

// OpenCensusUnmarshaller implements Unmarshaller
type OpenCensusUnmarshaller struct{}

// NewOpenCensusUnmarshaller constructs a OpenCensusUnmarshaller
func NewOpenCensusUnmarshaller() *OpenCensusUnmarshaller {
	return &OpenCensusUnmarshaller{}
}

// Unmarshal decodes a protobuf byte array to a span
func (h *OpenCensusUnmarshaller) Unmarshal(msg []byte) ([]*model.Span, error) {
	// load opencensus spans
	ds := &dumpproto.DumpSpans{}
	err := proto.Unmarshal(msg, ds)
	if err != nil {
		return nil, err
	}

	spans := make([]*model.Span, 0, len(ds.Spans))

	for _, span := range ds.Spans {
		if span != nil {
			// validation
			if span.StartTime == nil || span.EndTime == nil ||
				len(span.TraceId) != 16 || len(span.SpanId) != 8 {
				continue
			}
			startTime := time.Unix(span.StartTime.Seconds, int64(span.StartTime.Nanos))
			endTime := time.Unix(span.EndTime.Seconds, int64(span.EndTime.Nanos))

			spans = append(spans,
				&model.Span{
					TraceID:       model.NewTraceID(binary.BigEndian.Uint64(span.TraceId[0:8]), binary.BigEndian.Uint64(span.TraceId[8:16])),
					SpanID:        model.NewSpanID(binary.BigEndian.Uint64(span.SpanId)),
					OperationName: strings.ToUpper(extractRemoteKind(span)) + "::" + operationName(span),
					References:    convertReferences(span),
					Flags:         0,
					StartTime:     startTime,
					Duration:      endTime.Sub(startTime),
					Tags:          convertTags(span),
					Logs:          convertLogs(span),
					Process:       &model.Process{ServiceName: extractServiceName(span)},
				},
			)
		}
	}
	return spans, err
}

func convertReferences(span *traceproto.Span) []model.SpanRef {
	var spanRefs []model.SpanRef
	if span.Links != nil {
		spanRefs = make([]model.SpanRef, 0, len(span.Links.Link)+1)
		for _, link := range span.Links.Link {
			if link != nil {
				if len(link.TraceId) != 16 || len(link.SpanId) != 8 {
					continue
				}
				spanRefs = append(spanRefs, model.SpanRef{
					TraceID: model.NewTraceID(binary.BigEndian.Uint64(link.TraceId[0:8]), binary.BigEndian.Uint64(link.TraceId[8:16])),
					SpanID:  model.NewSpanID(binary.BigEndian.Uint64(link.SpanId)),
					RefType: convertReferenceType(link.Type),
				},
				)
			}
		}
	}

	// add parent span id as a link
	if len(span.TraceId) == 16 && len(span.ParentSpanId) == 8 {
		if spanRefs == nil {
			spanRefs = make([]model.SpanRef, 0, 1)
		}
		spanRefs = append(spanRefs, model.SpanRef{
			TraceID: model.NewTraceID(binary.BigEndian.Uint64(span.TraceId[0:8]), binary.BigEndian.Uint64(span.TraceId[8:16])),
			SpanID:  model.NewSpanID(binary.BigEndian.Uint64(span.ParentSpanId)),
			RefType: model.SpanRefType_CHILD_OF,
		},
		)
	}
	return spanRefs
}

func convertReferenceType(t traceproto.Span_Link_Type) model.SpanRefType {
	switch t {
	case traceproto.Span_Link_CHILD_LINKED_SPAN:
		return model.SpanRefType_FOLLOWS_FROM
	case traceproto.Span_Link_PARENT_LINKED_SPAN:
		return model.SpanRefType_CHILD_OF
	}
	return model.SpanRefType_FOLLOWS_FROM
}

func extractServiceName(span *traceproto.Span) string {
	serviceName := "unset"
	if span.Attributes != nil {
		if v, ok := span.Attributes.AttributeMap[SERVICE_NAME_KEY]; ok {
			if v.GetStringValue() != nil {
				serviceName = v.GetStringValue().Value
			}
		}
	}
	return serviceName
}

func extractRemoteKind(span *traceproto.Span) string {
	remoteKind := "unset"
	if span.Attributes != nil {
		if v, ok := span.Attributes.AttributeMap[REMOTE_KIND_KEY]; ok {
			if v.GetStringValue() != nil {
				remoteKind = v.GetStringValue().Value
			}
		}
	}
	return remoteKind
}

func convertTags(span *traceproto.Span) []model.KeyValue {
	var tags []model.KeyValue

	if span.Attributes != nil {
		tags = make([]model.KeyValue, 0, len(span.Attributes.AttributeMap)+2)
		for k, v := range span.Attributes.AttributeMap {
			tag := attributeToTag(k, v)
			if tag != nil {
				tags = append(tags, *tag)
			}
		}
	}

	if span.Status != nil {
		if tags == nil {
			tags = make([]model.KeyValue, 0, 2)
		}
		tags = append(tags, model.KeyValue{Key: "status.code", VType: model.ValueType_INT64, VInt64: int64(span.Status.Code)},
			model.KeyValue{Key: "status.message", VType: model.ValueType_STRING, VStr: span.Status.Message})
	}
	return tags
}

func convertLogs(span *traceproto.Span) []model.Log {
	var logs []model.Log
	if span.TimeEvents != nil {
		logs = make([]model.Log, 0, len(span.TimeEvents.TimeEvent))
		for _, event := range span.TimeEvents.TimeEvent {
			if event != nil && event.Time != nil {
				switch event.Value.(type) {
				case *traceproto.Span_TimeEvent_Annotation_:
					annotation := event.GetAnnotation()
					if annotation != nil {
						var fields []model.KeyValue
						attributes := annotation.Attributes
						if attributes != nil {
							fields = make([]model.KeyValue, 0, len(attributes.AttributeMap)+1)
							for k, v := range attributes.AttributeMap {
								tag := attributeToTag(k, v)
								if tag != nil {
									fields = append(fields, *tag)
								}
							}
						}
						description := annotation.Description
						if description != nil {
							if fields == nil {
								fields = make([]model.KeyValue, 0, 1)
							}
							fields = append(fields, model.KeyValue{Key: "message", VType: model.ValueType_STRING, VStr: description.Value})
						}
						logs = append(logs, model.Log{Timestamp: time.Unix(event.Time.Seconds, int64(event.Time.Nanos)), Fields: fields})
					}
				case *traceproto.Span_TimeEvent_MessageEvent_:
					messageEvent := event.GetMessageEvent()
					if messageEvent != nil {
						fields := make([]model.KeyValue, 0, 4)
						fields = append(fields,
							model.KeyValue{Key: "Id", VType: model.ValueType_INT64, VInt64: int64(messageEvent.Id)},
							model.KeyValue{Key: "UncompressedSize", VType: model.ValueType_INT64, VInt64: int64(messageEvent.UncompressedSize)},
							model.KeyValue{Key: "CompressedSize", VType: model.ValueType_INT64, VInt64: int64(messageEvent.CompressedSize)},
							model.KeyValue{Key: "Type", VType: model.ValueType_STRING, VStr: messageEventTypeToStr(messageEvent.Type)})
						logs = append(logs, model.Log{Timestamp: time.Unix(event.Time.Seconds, int64(event.Time.Nanos)), Fields: fields})
					}
				}
			}
		}
	}
	return logs
}

func attributeToTag(key string, attr *traceproto.AttributeValue) *model.KeyValue {
	if attr != nil {
		switch attr.Value.(type) {
		case *traceproto.AttributeValue_StringValue:
			if attr.GetStringValue() != nil {
				return &model.KeyValue{Key: key, VType: model.ValueType_STRING, VStr: attr.GetStringValue().Value}
			}
		case *traceproto.AttributeValue_IntValue:
			return &model.KeyValue{Key: key, VType: model.ValueType_INT64, VInt64: attr.GetIntValue()}
		case *traceproto.AttributeValue_BoolValue:
			return &model.KeyValue{Key: key, VType: model.ValueType_BOOL, VBool: attr.GetBoolValue()}
		}
	}
	return nil
}

func operationName(span *traceproto.Span) string {
	n := "unknown"
	if span.Name != nil {
		n = span.Name.Value
	}
	switch span.Kind {
	case traceproto.Span_CLIENT:
		n = "Sent." + n
	case traceproto.Span_SERVER:
		n = "Recv." + n
	}
	return n
}

func messageEventTypeToStr(t traceproto.Span_TimeEvent_MessageEvent_Type) string {
	switch t {
	case traceproto.Span_TimeEvent_MessageEvent_SENT:
		return "SENT"
	case traceproto.Span_TimeEvent_MessageEvent_RECEIVED:
		return "RECEIVED"
	}
	return "UNSPECIFIED"
}
