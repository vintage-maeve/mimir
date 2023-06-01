// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/reader_pool.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// ReaderPoolMetrics holds metrics tracked by ReaderPool.
type ReaderPoolMetrics struct {
	lazyReader   *LazyBinaryReaderMetrics
	streamReader *StreamBinaryReaderMetrics
}

// NewReaderPoolMetrics makes new ReaderPoolMetrics.
func NewReaderPoolMetrics(reg prometheus.Registerer) *ReaderPoolMetrics {
	return &ReaderPoolMetrics{
		lazyReader:   NewLazyBinaryReaderMetrics(reg),
		streamReader: NewStreamBinaryReaderMetrics(reg),
	}
}

// ReaderPool is used to istantiate new index-header readers and keep track of them.
// When the lazy reader is enabled, the pool keeps track of all instantiated readers
// and automatically close them once the idle timeout is reached. A closed lazy reader
// will be automatically re-opened upon next usage.
type ReaderPool struct {
	lazyReaderEnabled     bool
	lazyReaderIdleTimeout time.Duration
	logger                log.Logger
	metrics               *ReaderPoolMetrics

	// Channel used to signal once the pool is closing.
	close chan struct{}

	// Keep track of all readers managed by the pool.
	lazyReadersMx sync.Mutex
	lazyReaders   map[*LazyBinaryReader]struct{}

	lazyLoadedTracker HeadersLazyLoadedTracker
}

// HeadersLazyLoadedTracker persists the list of lazy loaded blocks.
type HeadersLazyLoadedTracker struct {
	State storepb.HeadersLazyLoadedTrackerState
	Path  string
}

// CopyLazyLoadedState copies the list of lazy loaded block to map tracked by State.
func (h *HeadersLazyLoadedTracker) CopyLazyLoadedState(lazyReaders map[*LazyBinaryReader]struct{}) {
	h.State.LazyLoadedBlocks = make(map[string]int64)
	for k := range lazyReaders {
		if k.reader != nil {
			h.State.LazyLoadedBlocks[k.blockId] = k.usedAt.Load() / int64(time.Millisecond)
		}
	}
}

func (h *HeadersLazyLoadedTracker) Persist() error {
	data, err := h.State.Marshal()
	if err != nil {
		return err
	}

	return os.WriteFile(h.Path, data, 0600)
}

// NewReaderPool makes a new ReaderPool and starts a background task for unloading idle Readers and blockIds writers if enabled.
func NewReaderPool(logger log.Logger, lazyReaderEnabled bool, lazyReaderIdleTimeout time.Duration, metrics *ReaderPoolMetrics, lazyLoadedTracker HeadersLazyLoadedTracker) *ReaderPool {
	p := newReaderPool(logger, lazyReaderEnabled, lazyReaderIdleTimeout, metrics, lazyLoadedTracker)

	// Start a goroutine to close idle readers (only if required).
	if p.lazyReaderEnabled && p.lazyReaderIdleTimeout > 0 {
		checkFreq := p.lazyReaderIdleTimeout / 10

		go func() {
			for {
				select {
				case <-p.close:
					return
				case <-time.After(checkFreq):
					p.closeIdleReaders()
				}
			}
		}()

		go func() {
			for {
				select {
				case <-p.close:
					return
				// TODO: this should be 1 minute
				case <-time.After(2 * time.Second):
					// We copy the state of blocks that are being lazy loaded from LazyBinaryReaders.
					p.lazyReadersMx.Lock()
					p.lazyLoadedTracker.CopyLazyLoadedState(p.lazyReaders)
					p.lazyReadersMx.Unlock()

					// Then we persist the state to files.
					p.lazyLoadedTracker.Persist()
				}
			}
		}()
	}

	return p
}

// newReaderPool makes a new ReaderPool.
func newReaderPool(logger log.Logger, lazyReaderEnabled bool, lazyReaderIdleTimeout time.Duration, metrics *ReaderPoolMetrics, lazyLoadedTracker HeadersLazyLoadedTracker) *ReaderPool {
	return &ReaderPool{
		logger:                logger,
		metrics:               metrics,
		lazyReaderEnabled:     lazyReaderEnabled,
		lazyReaderIdleTimeout: lazyReaderIdleTimeout,
		lazyReaders:           make(map[*LazyBinaryReader]struct{}),
		close:                 make(chan struct{}),
		lazyLoadedTracker:     lazyLoadedTracker,
	}
}

// NewBinaryReader creates and returns a new binary reader. If the pool has been configured
// with lazy reader enabled, this function will return a lazy reader. The returned lazy reader
// is tracked by the pool and automatically closed once the idle timeout expires.
func (p *ReaderPool) NewBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, cfg Config) (Reader, error) {
	var readerFactory func() (Reader, error)
	var reader Reader
	var err error

	readerFactory = func() (Reader, error) {
		return NewStreamBinaryReader(ctx, logger, bkt, dir, id, postingOffsetsInMemSampling, p.metrics.streamReader, cfg)
	}

	if p.lazyReaderEnabled {
		reader, err = NewLazyBinaryReader(ctx, readerFactory, logger, bkt, dir, id, p.metrics.lazyReader, p.onLazyReaderClosed)
	} else {
		reader, err = readerFactory()
	}

	if err != nil {
		return nil, err
	}

	// Keep track of lazy readers only if required.
	if p.lazyReaderEnabled && p.lazyReaderIdleTimeout > 0 {
		p.lazyReadersMx.Lock()
		p.lazyReaders[reader.(*LazyBinaryReader)] = struct{}{}
		p.lazyReadersMx.Unlock()
	}

	return reader, err
}

// Close the pool and stop checking for idle readers. No reader tracked by this pool
// will be closed. It's the caller responsibility to close readers.
func (p *ReaderPool) Close() {
	close(p.close)
}

func (p *ReaderPool) closeIdleReaders() {
	idleTimeoutAgo := time.Now().Add(-p.lazyReaderIdleTimeout).UnixNano()

	for _, r := range p.getIdleReadersSince(idleTimeoutAgo) {
		if err := r.unloadIfIdleSince(idleTimeoutAgo); err != nil && !errors.Is(err, errNotIdle) {
			level.Warn(p.logger).Log("msg", "failed to close idle index-header reader", "err", err)
		}
	}
}

func (p *ReaderPool) getIdleReadersSince(ts int64) []*LazyBinaryReader {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	var idle []*LazyBinaryReader
	for r := range p.lazyReaders {
		if r.isIdleSince(ts) {
			idle = append(idle, r)
		}
	}

	return idle
}

func (p *ReaderPool) isTracking(r *LazyBinaryReader) bool {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	_, ok := p.lazyReaders[r]
	return ok
}

func (p *ReaderPool) onLazyReaderClosed(r *LazyBinaryReader) {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	// When this function is called, it means the reader has been closed NOT because was idle
	// but because the consumer closed it. By contract, a reader closed by the consumer can't
	// be used anymore, so we can automatically remove it from the pool.
	delete(p.lazyReaders, r)
}
