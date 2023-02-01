// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/limiter/query_limiter.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package limiter

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

type queryLimiterCtxKey struct{}

var (
	ctxKey                = &queryLimiterCtxKey{}
	MaxSeriesHitMsgFormat = globalerror.MaxSeriesPerQuery.MessageWithPerTenantLimitConfig(
		"the query exceeded the maximum number of series (limit: %d series)",
		validation.MaxSeriesPerQueryFlag,
	)
	MaxChunkBytesHitMsgFormat = globalerror.MaxChunkBytesPerQuery.MessageWithPerTenantLimitConfig(
		"the query exceeded the aggregated chunks size limit (limit: %d bytes)",
		validation.MaxChunkBytesPerQueryFlag,
	)
	MaxChunksPerQueryLimitMsgFormat = globalerror.MaxChunksPerQuery.MessageWithPerTenantLimitConfig(
		"the query exceeded the maximum number of chunks (limit: %d chunks)",
		validation.MaxChunksPerQueryFlag,
	)
)

type QueryLimiter struct {
	uniqueSeriesMx sync.Mutex
	uniqueSeries   map[uint64]struct{}

	chunkBytesCount atomic.Int64
	chunkCount      atomic.Int64

	maxSeriesPerQuery     int
	maxChunkBytesPerQuery int
	maxChunksPerQuery     int

	logger log.Logger
}

// NewQueryLimiter makes a new per-query limiter. Each query limiter
// is configured using the `maxSeriesPerQuery` limit.
func NewQueryLimiter(maxSeriesPerQuery, maxChunkBytesPerQuery, maxChunksPerQuery int, logger log.Logger) *QueryLimiter {
	return &QueryLimiter{
		uniqueSeriesMx: sync.Mutex{},
		uniqueSeries:   map[uint64]struct{}{},

		maxSeriesPerQuery:     maxSeriesPerQuery,
		maxChunkBytesPerQuery: maxChunkBytesPerQuery,
		maxChunksPerQuery:     maxChunksPerQuery,
		logger:                logger,
	}
}

func AddQueryLimiterToContext(ctx context.Context, limiter *QueryLimiter) context.Context {
	return context.WithValue(ctx, ctxKey, limiter)
}

// QueryLimiterFromContextWithFallback returns a QueryLimiter from the current context.
// If there is not a QueryLimiter on the context it will return a new no-op limiter.
func QueryLimiterFromContextWithFallback(ctx context.Context) *QueryLimiter {
	ql, ok := ctx.Value(ctxKey).(*QueryLimiter)
	if !ok {
		// If there's no limiter return a new unlimited limiter as a fallback
		ql = NewQueryLimiter(0, 0, 0, nil)
	}
	return ql
}

// AddSeries adds the input series and returns an error if the limit is reached.
func (ql *QueryLimiter) AddSeries(seriesLabels []mimirpb.LabelAdapter) error {
	// If the max series is unlimited just return without managing map
	if ql.maxSeriesPerQuery == 0 {
		return nil
	}
	fingerprint := mimirpb.FromLabelAdaptersToLabels(seriesLabels).Hash()

	ql.uniqueSeriesMx.Lock()
	defer ql.uniqueSeriesMx.Unlock()

	_, alreadyLogged := ql.uniqueSeries[fingerprint]

	ql.uniqueSeries[fingerprint] = struct{}{}
	if len(ql.uniqueSeries) > ql.maxSeriesPerQuery {
		// Format error with max limit
		if ql.logger != nil {
			level.Warn(ql.logger).Log("source", "query_limiter.go", "func", "AddSeries", "msg", string(len(seriesLabels))+"Label adapters converted to a series and added to the limiter", "uniqueSeries", len(ql.uniqueSeries), "maxSeriesPerQuery", ql.maxSeriesPerQuery, "status", "FAILED")
		}
		return fmt.Errorf(MaxSeriesHitMsgFormat, ql.maxSeriesPerQuery)
	}
	if len(seriesLabels) != 0 && ql.logger != nil && !alreadyLogged {
		level.Warn(ql.logger).Log("source", "query_limiter.go", "func", "AddSeries", "msg", string(len(seriesLabels))+"Label adapters converted to a series and added to the limiter", "uniqueSeries", len(ql.uniqueSeries), "maxSeriesPerQuery", ql.maxSeriesPerQuery, "status", "OK")
	}
	return nil
}

// uniqueSeriesCount returns the count of unique series seen by this query limiter.
func (ql *QueryLimiter) uniqueSeriesCount() int {
	ql.uniqueSeriesMx.Lock()
	defer ql.uniqueSeriesMx.Unlock()
	return len(ql.uniqueSeries)
}

// AddChunkBytes adds the input chunk size in bytes and returns an error if the limit is reached.
func (ql *QueryLimiter) AddChunkBytes(chunkSizeInBytes int) error {
	if ql.maxChunkBytesPerQuery == 0 {
		return nil
	}
	if ql.chunkBytesCount.Add(int64(chunkSizeInBytes)) > int64(ql.maxChunkBytesPerQuery) {
		return fmt.Errorf(MaxChunkBytesHitMsgFormat, ql.maxChunkBytesPerQuery)
	}
	return nil
}

func (ql *QueryLimiter) AddChunks(count int) error {
	if ql.maxChunksPerQuery == 0 {
		return nil
	}

	if ql.chunkCount.Add(int64(count)) > int64(ql.maxChunksPerQuery) {
		if ql.logger != nil {
			level.Warn(ql.logger).Log("source", "query_limiter.go", "func", "AddChunks", "msg", "Adding "+string(count)+" chunks to the limiter", "chunkCount", ql.chunkCount.Load(), "maxChunksPerQuery", ql.maxChunksPerQuery, "status", "FAILED")
		}
		return fmt.Errorf(MaxChunksPerQueryLimitMsgFormat, ql.maxChunksPerQuery)
	}
	if count != 0 && ql.logger != nil {
		level.Warn(ql.logger).Log("source", "query_limiter.go", "func", "AddChunks", "msg", "Adding "+string(count)+" chunks to the limiter", "chunkCount", ql.chunkCount.Load(), "maxChunksPerQuery", ql.maxChunksPerQuery, "status", "OK")
	}
	return nil
}
