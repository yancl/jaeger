// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tracing

import (
	"fmt"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/uber/jaeger-client-go/rpcmetrics"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/transport/zipkin"
	"github.com/uber/jaeger/examples/hotrod/pkg/log"
)

// Init creates a new instance of Jaeger tracer.
func Init(serviceName string, metricsFactory metrics.Factory, logger log.Factory) opentracing.Tracer {
	jaegerLogger := jaegerLoggerAdapter{logger.Bg()}
	metricsObserver := rpcmetrics.NewObserver(metricsFactory, rpcmetrics.DefaultNameNormalizer)

	sampler := jaeger.NewConstSampler(true)

	username := ""
	password := ""
	cannotInit := "Cannot initialize Jaeger Tracer"

	dcaReporter, err := newReporter("dca1", username, password, jaegerLogger)
	if err != nil {
		logger.Bg().Fatal(cannotInit, zap.Error(err))
	}

	sjcReporter, err := newReporter("sjc1", username, password, jaegerLogger)
	if err != nil {
		logger.Bg().Fatal(cannotInit, zap.Error(err))
	}

	loggingReporter := jaeger.NewLoggingReporter(jaegerLogger) //optional, for debugging

	compositeReporter := jaeger.NewCompositeReporter(dcaReporter, sjcReporter, loggingReporter)

	tracer, _ := jaeger.NewTracer("http-reporter-test-"+serviceName, sampler, compositeReporter, jaeger.TracerOptions.Observer(metricsObserver))

	if err != nil {
		logger.Bg().Fatal(cannotInit, zap.Error(err))
	}
	return tracer
}

func newReporter(dc, username, password string, logger jaeger.Logger) (jaeger.Reporter, error) {
	url := fmt.Sprintf("https://jaeger-%s.uber.com/api/traces?format=zipkin.thrift", dc)
	t, err := zipkin.NewHTTPTransport(url, zipkin.HTTPBasicAuth(username, password))
	if err != nil {
		return nil, err
	}
	r := jaeger.NewRemoteReporter(t, jaeger.ReporterOptions.Logger(logger))
	return debugReporter{reporter: r}, nil
}

// debugReporter sets the sampling.priority of every span to 1, this ensures that these spans aren't discarded by
// jaeger collectors during downsampling
type debugReporter struct {
	reporter jaeger.Reporter
}

func (d debugReporter) Report(span *jaeger.Span) {
	ext.SamplingPriority.Set(span, 1)
	d.reporter.Report(span)
}

func (d debugReporter) Close() {
	d.reporter.Close()
}

type jaegerLoggerAdapter struct {
	logger log.Logger
}

func (l jaegerLoggerAdapter) Error(msg string) {
	l.logger.Error(msg)
}

func (l jaegerLoggerAdapter) Infof(msg string, args ...interface{}) {
	l.logger.Info(fmt.Sprintf(msg, args...))
}
