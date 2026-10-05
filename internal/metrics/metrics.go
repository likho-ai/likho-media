// Package metrics is what the service tells about itself (OpenTelemetry): always as Prometheus
// text at GET /metrics, and pushed over OTLP/HTTP as well when an endpoint is set.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Metrics holds the instruments the service records into.
type Metrics struct {
	provider *sdkmetric.MeterProvider
	registry *prometheus.Registry

	// Uploads received, by outcome: stored, duplicate, rejected.
	Uploads metric.Int64Counter
	// Bytes of audio received.
	UploadBytes metric.Int64Counter
	// Conversions finished, by outcome: ready, failed, retry.
	Conversions metric.Int64Counter
	// How long a conversion took, in seconds.
	ConversionSeconds metric.Float64Histogram
	// Events published to the bus, by subject.
	EventsPublished metric.Int64Counter
}

// Counts answers how many media are in each status.
type Counts func(ctx context.Context) (map[string]int, error)

// New builds the instruments. otlpEndpoint empty = only /metrics.
func New(ctx context.Context, service, version, otlpEndpoint string, counts Counts) (*Metrics, error) {
	registry := prometheus.NewRegistry()
	promReader, err := otelprom.New(otelprom.WithRegisterer(registry), otelprom.WithoutScopeInfo())
	if err != nil {
		return nil, fmt.Errorf("metrics: %w", err)
	}
	options := []sdkmetric.Option{
		sdkmetric.WithReader(promReader),
		sdkmetric.WithResource(resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(service), semconv.ServiceVersion(version))),
	}
	if otlpEndpoint != "" {
		exporter, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpointURL(strings.TrimRight(otlpEndpoint, "/")+"/v1/metrics"))
		if err != nil {
			return nil, fmt.Errorf("metrics: %w", err)
		}
		options = append(options, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			sdkmetric.WithInterval(15*time.Second))))
	}
	provider := sdkmetric.NewMeterProvider(options...)
	meter := provider.Meter(service)

	m := &Metrics{provider: provider, registry: registry}
	if m.Uploads, err = meter.Int64Counter("likho_media_uploads",
		metric.WithDescription("Uploads received, by outcome")); err != nil {
		return nil, err
	}
	if m.UploadBytes, err = meter.Int64Counter("likho_media_upload_bytes",
		metric.WithDescription("Bytes of audio received")); err != nil {
		return nil, err
	}
	if m.Conversions, err = meter.Int64Counter("likho_media_conversions",
		metric.WithDescription("Conversions finished, by outcome")); err != nil {
		return nil, err
	}
	if m.ConversionSeconds, err = meter.Float64Histogram("likho_media_conversion_seconds",
		metric.WithDescription("How long a conversion took"),
		metric.WithExplicitBucketBoundaries(0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300)); err != nil {
		return nil, err
	}
	if m.EventsPublished, err = meter.Int64Counter("likho_media_events_published",
		metric.WithDescription("Events published to the bus, by subject")); err != nil {
		return nil, err
	}
	gauge, err := meter.Int64ObservableGauge("likho_media", metric.WithDescription("Media by status"))
	if err != nil {
		return nil, err
	}
	if _, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		byStatus, err := counts(ctx)
		if err != nil {
			return err
		}
		for status, n := range byStatus {
			o.ObserveInt64(gauge, int64(n), metric.WithAttributes(attribute.String("status", status)))
		}
		return nil
	}, gauge); err != nil {
		return nil, err
	}
	return m, nil
}

// Handler serves the Prometheus text.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Outcome is the attribute set for an outcome label.
func Outcome(value string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("outcome", value))
}

// Subject is the attribute set for a bus subject.
func Subject(value string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("subject", value))
}

// Close flushes what is pending to the OTLP endpoint, if any.
func (m *Metrics) Close(ctx context.Context) error {
	return m.provider.Shutdown(ctx)
}
