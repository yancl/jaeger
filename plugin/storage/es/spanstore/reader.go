package spanstore

import (
	"context"
	"time"
	"encoding/json"

	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	jConverter "github.com/uber/jaeger/model/converter/json"
	jModel "github.com/uber/jaeger/model/json"
	"github.com/uber/jaeger/pkg/es"
	"github.com/uber/jaeger/model"
	"github.com/uber/jaeger/storage/spanstore"
)

const (
	serviceName           = "serviceName"
	indexPrefix           = "jaeger-"
	operationsAggregation = "distinct_operations"
	servicesAggregation   = "distinct_services"
	defaultDocCount       = 3000
)

// SpanReader can query for and load traces from ElasticSearch
type SpanReader struct {
	ctx    context.Context
	client es.Client
	logger *zap.Logger
}

// NewSpanReader returns a new SpanReader.
func NewSpanReader(client es.Client, logger *zap.Logger) *SpanReader {
	ctx := context.Background()
	return &SpanReader{
		ctx:    ctx,
		client: client,
		logger: logger,
	}
}

// GetTrace takes a traceID and returns a Trace associated with that traceID
func (s *SpanReader) GetTrace(traceID model.TraceID) (*model.Trace, error) {
	return s.readTrace(traceID.String(), spanstore.TraceQueryParameters{})
}

func (s *SpanReader) readTrace(traceID string, traceQuery spanstore.TraceQueryParameters) (*model.Trace, error) {
	query := elastic.NewTermQuery("traceID", traceID)

	indices := s.findIndices(traceQuery)
	esSpansRaw, err := s.executeQuery(query, indices...)
	if err != nil {
		return nil, err
	}
	if len(esSpansRaw) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}

	spans := make([]*model.Span, len(esSpansRaw))

	for i, esSpanRaw := range esSpansRaw {
		jsonSpan, err := s.esJSONtoJSONSpanModel(esSpanRaw)
		span, err := jConverter.SpanToDomain(jsonSpan)
		if err != nil {
			return nil, err
		}
		spans[i] = span
	}

	trace := &model.Trace{}
	trace.Spans = spans
	return trace, nil
}

func (s *SpanReader) executeQuery(query elastic.Query, indices ...string) ([]*elastic.SearchHit, error) {
	searchService, err := s.client.Search(indices...).
		Type(spanType).
		Query(query).
		Do(s.ctx)
	if err != nil {
		return nil, err
	}
	return searchService.Hits.Hits, nil
}

func (s *SpanReader) esJSONtoJSONSpanModel(esSpanRaw *elastic.SearchHit) (*jModel.Span, error) {
	esSpanInByteArray:= esSpanRaw.Source

	var jsonSpan jModel.Span
	err := json.Unmarshal(*esSpanInByteArray, &jsonSpan)
	if err != nil {
		return nil, err
	}
	return &jsonSpan, nil
}


// Returns the array of indices that we need to query, based on query params
func (s *SpanReader) findIndices(traceQuery spanstore.TraceQueryParameters) []string {
	today := time.Now()
	threeDaysAgo := today.AddDate(0, 0, -3) // TODO: make this configurable

	if traceQuery.StartTimeMax.IsZero() || traceQuery.StartTimeMin.IsZero() {
		traceQuery.StartTimeMax = today
		traceQuery.StartTimeMin = threeDaysAgo
	}

	var indices []string
	current := traceQuery.StartTimeMax
	for current.After(traceQuery.StartTimeMin) && current.After(threeDaysAgo) {
		index := s.indexWithDate(current)
		exists, _ := s.client.IndexExists(index).Do(s.ctx) // Don't care about error, if it's an error, exists will be false anyway
		if exists {
			indices = append(indices, index)
		}
		current = current.AddDate(0, 0, -1)
	}
	return indices
}

func (s *SpanReader) indexWithDate(date time.Time) string {
	return indexPrefix + date.Format("2006-01-02")
}

// GetServices returns all services traced by Jaeger, ordered by frequency
func (s *SpanReader) GetServices() ([]string, error) {
	serviceAggregation := s.getServicesAggregation()

	jaegerIndices := s.findIndices(spanstore.TraceQueryParameters{})

	searchService := s.client.Search(jaegerIndices...).
		Type(serviceType).
		Size(0). // set to 0 because we don't want actual documents.
		Aggregation(servicesAggregation, serviceAggregation)

	searchResult, err := searchService.Do(s.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Search service failed")
	}

	bucket, found := searchResult.Aggregations.Terms(servicesAggregation)
	if !found {
		return nil, errors.New("Could not find aggregation of services")
	}
	serviceNamesBucket := bucket.Buckets
	return s.bucketToStringArray(serviceNamesBucket)
}

func (s *SpanReader) getServicesAggregation() elastic.Query {
	return elastic.NewTermsAggregation().
		Field(serviceName).
		Size(defaultDocCount) // Must set to some large number. ES deprecated size omission for aggregating all. https://github.com/elastic/elasticsearch/issues/18838
}

// GetOperations returns all operations for a specific service traced by Jaeger
func (s *SpanReader) GetOperations(service string) ([]string, error) {
	serviceQuery := elastic.NewTermQuery(serviceName, service)
	serviceFilter := elastic.NewFilterAggregation().Filter(serviceQuery)
	jaegerIndices := s.findIndices(spanstore.TraceQueryParameters{})

	searchService := s.client.Search(jaegerIndices...).
		Type(serviceType).
		Size(0).
		Aggregation(operationsAggregation, serviceFilter)

	searchResult, err := searchService.Do(s.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Search service failed")
	}
	bucket, found := searchResult.Aggregations.Terms(operationsAggregation)
	if !found {
		return nil, errors.New("Could not find aggregation of operations")
	}
	operationNamesBucket := bucket.Buckets
	return s.bucketToStringArray(operationNamesBucket)
}

func (s *SpanReader) bucketToStringArray(buckets []*elastic.AggregationBucketKeyItem) ([]string, error) {
	strings := make([]string, len(buckets))
	for i, keyitem := range buckets {
		str, ok := keyitem.Key.(string)
		if !ok {
			return nil, errors.New("Non-string key found in aggregation")
		}
		strings[i] = str
	}
	return strings, nil
}

// FindTraces retrieves traces that match the traceQuery
func (s *SpanReader) FindTraces(traceQuery *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	// TODO
	return nil, nil
}