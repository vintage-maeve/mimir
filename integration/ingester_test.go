// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestIngesterQuerying(t *testing.T) {
	query := "foobar"
	queryEnd := time.Now().Round(time.Second)
	queryStart := queryEnd.Add(-1 * time.Hour)
	queryStep := 10 * time.Minute

	testCases := map[string]struct {
		inSeries []prompb.TimeSeries
		expected model.Matrix
	}{
		"float series": {
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "foobar",
						},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: queryStart.UnixMilli(),
							Value:     100,
						},
						{
							Timestamp: queryStart.Add(queryStep).UnixMilli(),
							Value:     110,
						},
						{
							Timestamp: queryStart.Add(queryStep * 2).UnixMilli(),
							Value:     120,
						},
						{
							Timestamp: queryStart.Add(queryStep * 3).UnixMilli(),
							Value:     130,
						},
						{
							Timestamp: queryStart.Add(queryStep * 4).UnixMilli(),
							Value:     140,
						},
						{
							Timestamp: queryStart.Add(queryStep * 5).UnixMilli(),
							Value:     150,
						},
					},
				},
			},
			expected: model.Matrix{
				{
					Metric: model.Metric{"__name__": "foobar"},
					Values: []model.SamplePair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Value:     model.SampleValue(100),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Value:     model.SampleValue(110),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Value:     model.SampleValue(120),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Value:     model.SampleValue(130),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Value:     model.SampleValue(140),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Value:     model.SampleValue(150),
						},
					},
				},
			},
		},
		"integer histogram series": {
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "foobar",
						},
					},
					Histograms: []prompb.Histogram{
						remote.HistogramToHistogramProto(queryStart.UnixMilli(), test.GenerateTestHistogram(1)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep).UnixMilli(), test.GenerateTestHistogram(2)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*2).UnixMilli(), test.GenerateTestHistogram(3)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*3).UnixMilli(), test.GenerateTestHistogram(4)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*4).UnixMilli(), test.GenerateTestHistogram(5)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*5).UnixMilli(), test.GenerateTestHistogram(6)),
					},
				},
			},
			expected: model.Matrix{
				{
					Metric: model.Metric{"__name__": "foobar"},
					Histograms: []model.SampleHistogramPair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(1),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(2),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(3),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(4),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(5),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(6),
						},
					},
				},
			},
		},
		"float histogram series": {
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "foobar",
						},
					},
					Histograms: []prompb.Histogram{
						remote.FloatHistogramToHistogramProto(queryStart.UnixMilli(), test.GenerateTestFloatHistogram(1)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep).UnixMilli(), test.GenerateTestFloatHistogram(2)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*2).UnixMilli(), test.GenerateTestFloatHistogram(3)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*3).UnixMilli(), test.GenerateTestFloatHistogram(4)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*4).UnixMilli(), test.GenerateTestFloatHistogram(5)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*5).UnixMilli(), test.GenerateTestFloatHistogram(6)),
					},
				},
			},
			expected: model.Matrix{
				{
					Metric: model.Metric{"__name__": "foobar"},
					Histograms: []model.SampleHistogramPair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(1),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(2),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(3),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(4),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(5),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(6),
						},
					},
				},
			},
		},
		"series switching from float to integer histogram to float histogram": {
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "foobar",
						},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: queryStart.UnixMilli(),
							Value:     100,
						},
						{
							Timestamp: queryStart.Add(queryStep).UnixMilli(),
							Value:     110,
						},
					},
					Histograms: []prompb.Histogram{
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*2).UnixMilli(), test.GenerateTestHistogram(3)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*3).UnixMilli(), test.GenerateTestHistogram(4)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*4).UnixMilli(), test.GenerateTestFloatHistogram(5)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*5).UnixMilli(), test.GenerateTestFloatHistogram(6)),
					},
				},
			},
			expected: model.Matrix{
				{
					Metric: model.Metric{"__name__": "foobar"},
					Values: []model.SamplePair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Value:     model.SampleValue(100),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Value:     model.SampleValue(110),
						},
					},
					Histograms: []model.SampleHistogramPair{
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(3),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(4),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(5),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(6),
						},
					},
				},
			},
		},
		"series including float and native histograms at same timestamp": {
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "foobar",
						},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: queryStart.UnixMilli(),
							Value:     100,
						},
						{
							Timestamp: queryStart.Add(queryStep).UnixMilli(),
							Value:     110,
						},
						{
							Timestamp: queryStart.Add(queryStep * 2).UnixMilli(),
							Value:     120,
						},
					},
					Histograms: []prompb.Histogram{
						// This first of these will fail to get appended because there's already a float sample for that timestamp.
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*2).UnixMilli(), test.GenerateTestHistogram(3)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*3).UnixMilli(), test.GenerateTestHistogram(4)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*4).UnixMilli(), test.GenerateTestHistogram(5)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*5).UnixMilli(), test.GenerateTestHistogram(6)),
					},
				},
			},
			expected: model.Matrix{
				{
					Metric: model.Metric{"__name__": "foobar"},
					Values: []model.SamplePair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Value:     model.SampleValue(100),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Value:     model.SampleValue(110),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Value:     model.SampleValue(120),
						},
					},
					Histograms: []model.SampleHistogramPair{
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(4),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(5),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(6),
						},
					},
				},
			},
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			for _, streamingEnabled := range []bool{true, false} {
				t.Run(fmt.Sprintf("streaming enabled: %v", streamingEnabled), func(t *testing.T) {
					s, err := e2e.NewScenario(networkName)
					require.NoError(t, err)
					defer s.Close()

					baseFlags := map[string]string{
						"-distributor.ingestion-tenant-shard-size": "0",
						"-ingester.ring.heartbeat-period":          "1s",
						"-querier.prefer-streaming-chunks":         strconv.FormatBool(streamingEnabled),
					}

					flags := mergeFlags(
						BlocksStorageFlags(),
						BlocksStorageS3Flags(),
						baseFlags,
					)

					// Start dependencies.
					consul := e2edb.NewConsul()
					minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
					require.NoError(t, s.StartAndWaitReady(consul, minio))

					// Start Mimir components.
					distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
					ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
					querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
					require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

					// Wait until distributor has updated the ring.
					require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
						labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

					// Wait until querier has updated the ring.
					require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
						labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

					client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
					require.NoError(t, err)

					res, err := client.Push(tc.inSeries)
					require.NoError(t, err)
					require.Equal(t, http.StatusOK, res.StatusCode)

					result, err := client.QueryRange(query, queryStart, queryEnd, queryStep)
					require.NoError(t, err)

					require.Equal(t, tc.expected.String(), result.String())
				})
			}
		})
	}
}

func TestIngesterQueryingWithRequestMinimization(t *testing.T) {
	for _, streamingEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("streaming enabled: %v", streamingEnabled), func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			baseFlags := map[string]string{
				"-distributor.ingestion-tenant-shard-size": "0",
				"-ingester.ring.heartbeat-period":          "1s",
				"-ingester.ring.zone-awareness-enabled":    "true",
				"-ingester.ring.replication-factor":        "3",
				"-querier.minimize-ingester-requests":      "true",
				"-querier.prefer-streaming-chunks":         strconv.FormatBool(streamingEnabled),
			}

			flags := mergeFlags(
				BlocksStorageFlags(),
				BlocksStorageS3Flags(),
				baseFlags,
			)

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			ingesterFlags := func(zone string) map[string]string {
				return mergeFlags(flags, map[string]string{
					"-ingester.ring.instance-availability-zone": zone,
				})
			}

			// Start Mimir components.
			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
			ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
			ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
			ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3, querier))

			// Wait until distributor and querier have updated the ring.
			for _, component := range []*e2emimir.MimirService{distributor, querier} {
				require.NoError(t, component.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
					labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
					labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))
			}

			client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			// Push some data to the cluster.
			seriesName := "test_series"
			now := time.Now()
			series, expectedVector, _ := generateFloatSeries(seriesName, now, prompb.Label{Name: "foo", Value: "bar"})

			res, err := client.Push(series)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			// Verify we can query the data we just pushed.
			queryResult, err := client.Query(seriesName, now)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, queryResult.Type())
			require.Equal(t, expectedVector, queryResult.(model.Vector))

			// Check that we only queried two of the three ingesters.
			totalQueryRequests := 0.0

			for _, ingester := range []*e2emimir.MimirService{ingester1, ingester2, ingester3} {
				sums, err := ingester.SumMetrics(
					[]string{"cortex_request_duration_seconds"},
					e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "route", "/cortex.Ingester/QueryStream"),
						labels.MustNewMatcher(labels.MatchEqual, "status_code", "success"),
					),
					e2e.SkipMissingMetrics,
					e2e.WithMetricCount,
				)

				require.NoError(t, err)
				queryRequests := sums[0]
				require.LessOrEqual(t, queryRequests, 1.0)
				totalQueryRequests += queryRequests
			}

			require.Equal(t, 2.0, totalQueryRequests)
		})
	}
}
