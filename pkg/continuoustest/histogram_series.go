package continuoustest

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

type MetricType string

const (
	Counter MetricType = "counter"
	Gauge   MetricType = "gauge"
)

type DataType string

const (
	IntHistogram   DataType = "int"
	FloatHistogram DataType = "float"
)

type HistogramType struct {
	MetricType MetricType
	DataType   DataType
}

const (
	// This separator is used to separate the metric type from the data type, ex: counter,int
	histogramTypeSeparator = ","
	// This separator is used to separate a sequence of histogram types, ex: counter,int;counter,float;gauge,int;gauge,float
	histogramTypesSeparator = ";"
)

func (ht *HistogramType) MarshalText() (text []byte, err error) {
	s := string(ht.MetricType) + histogramTypeSeparator + string(ht.DataType)
	return []byte(s), nil
}

func (ht *HistogramType) UnmarshalText(text []byte) error {
	s := strings.Split(string(text), histogramTypeSeparator)
	if len(s) != 2 {
		return fmt.Errorf("expected two parts (metrictype,datatype) but got %d: %s", len(s), text)
	}

	var mt MetricType
	switch s[0] {
	case "counter":
		mt = Counter
	case "gauge":
		mt = Gauge
	default:
		return fmt.Errorf("unrecognized metric type: %s", s[0])
	}

	var dt DataType
	switch s[1] {
	case "int":
		dt = IntHistogram
	case "float":
		dt = FloatHistogram
	default:
		return fmt.Errorf("unrecognized data type: %s", s[1])
	}

	ht.MetricType = mt
	ht.DataType = dt
	return nil
}

type HistogramTypes []HistogramType

func (hts *HistogramTypes) MarshalText() (text []byte, err error) {
	var types []string
	for _, ht := range *hts {
		t, err := ht.MarshalText()
		if err != nil {
			return nil, err
		}
		types = append(types, string(t))
	}
	return []byte(strings.Join(types, histogramTypesSeparator)), nil
}

func (hts *HistogramTypes) UnmarshalText(text []byte) error {
	types := strings.Split(string(text), histogramTypesSeparator)
	for _, t := range types {
		var ht HistogramType
		err := ht.UnmarshalText([]byte(t))
		if err != nil {
			return err
		}
		*hts = append(*hts, ht)
	}
	return nil
}

type HistogramSeriesConfig struct {
	NumSeries      int
	MaxQueryAge    time.Duration
	HistogramTypes HistogramTypes
}

func (cfg *HistogramSeriesConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.NumSeries, "tests.histogram-series.num-series", 10000, "Number of series used for the test.")
	f.DurationVar(&cfg.MaxQueryAge, "tests.histogram-series.max-query-age", 7*24*time.Hour, "How back in the past metrics can be queried at most.")
	f.TextVar(&cfg.HistogramTypes, "tests.histogram-series.histogram-type", &HistogramTypes{{Counter, IntHistogram}}, "The types of histograms to generate for the test (ex: counter,int;counter,float).")
}

type HistogramSeries struct {
	name    string
	cfg     HistogramSeriesConfig
	client  MimirClient
	logger  log.Logger
	metrics *TestMetrics
}

func NewHistogramSeries(cfg HistogramSeriesConfig, client MimirClient, logger log.Logger, reg prometheus.Registerer) *HistogramSeries {
	const name = "histogram-series"

	return &HistogramSeries{
		name:    name,
		cfg:     cfg,
		client:  client,
		logger:  log.With(logger, "test", name),
		metrics: NewTestMetrics(name, reg),
	}
}

func (t *HistogramSeries) Name() string {
	return t.name
}

func (t *HistogramSeries) Init(ctx context.Context, now time.Time) error {
	return nil // TODO
}

func (t *HistogramSeries) Run(ctx context.Context, now time.Time) error {
	return nil // TODO
}
