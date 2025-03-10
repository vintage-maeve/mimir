// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/2be2db77/pkg/compact/compact.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package compactor

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

type DeduplicateFilter interface {
	block.MetadataFilter

	// DuplicateIDs returns IDs of duplicate blocks generated by last call to Filter method.
	DuplicateIDs() []ulid.ULID
}

// Syncer synchronizes block metas from a bucket into a local directory.
// It sorts them into compaction groups based on equal label sets.
type Syncer struct {
	logger                  log.Logger
	bkt                     objstore.Bucket
	fetcher                 *block.MetaFetcher
	mtx                     sync.Mutex
	blocks                  map[ulid.ULID]*block.Meta
	metrics                 *syncerMetrics
	deduplicateBlocksFilter DeduplicateFilter
}

type syncerMetrics struct {
	garbageCollections        prometheus.Counter
	garbageCollectionFailures prometheus.Counter
	garbageCollectionDuration prometheus.Histogram
	blocksMarkedForDeletion   prometheus.Counter
}

func newSyncerMetrics(reg prometheus.Registerer, blocksMarkedForDeletion prometheus.Counter) *syncerMetrics {
	var m syncerMetrics

	m.garbageCollections = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_total",
		Help: "Total number of garbage collection operations.",
	})
	m.garbageCollectionFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_failures_total",
		Help: "Total number of failed garbage collection operations.",
	})
	m.garbageCollectionDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_compact_garbage_collection_duration_seconds",
		Help:    "Time it took to perform garbage collection iteration.",
		Buckets: []float64{0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120, 240, 360, 720},
	})

	m.blocksMarkedForDeletion = blocksMarkedForDeletion

	return &m
}

// NewMetaSyncer returns a new Syncer for the given Bucket and directory.
// Blocks must be at least as old as the sync delay for being considered.
func NewMetaSyncer(logger log.Logger, reg prometheus.Registerer, bkt objstore.Bucket, fetcher *block.MetaFetcher, deduplicateBlocksFilter DeduplicateFilter, blocksMarkedForDeletion prometheus.Counter) (*Syncer, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Syncer{
		logger:                  logger,
		bkt:                     bkt,
		fetcher:                 fetcher,
		blocks:                  map[ulid.ULID]*block.Meta{},
		metrics:                 newSyncerMetrics(reg, blocksMarkedForDeletion),
		deduplicateBlocksFilter: deduplicateBlocksFilter,
	}, nil
}

// SyncMetas synchronizes local state of block metas with what we have in the bucket.
func (s *Syncer) SyncMetas(ctx context.Context) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// While fetching blocks, we filter out blocks that were marked for deletion.
	// No deletion delay is used -- all blocks with deletion marker are ignored, and not considered for compaction.
	metas, _, err := s.fetcher.FetchWithoutMarkedForDeletion(ctx)
	if err != nil {
		return err
	}
	s.blocks = metas
	return nil
}

// Metas returns loaded metadata blocks since last sync.
func (s *Syncer) Metas() map[ulid.ULID]*block.Meta {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.blocks
}

// GarbageCollect marks blocks for deletion from bucket if their data is available as part of a
// block with a higher compaction level.
// Call to SyncMetas function is required to populate duplicateIDs in duplicateBlocksFilter.
func (s *Syncer) GarbageCollect(ctx context.Context) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	begin := time.Now()

	// The deduplication filter is applied after all blocks marked for deletion have been excluded
	// (with no deletion delay), so we expect that all duplicated blocks have not been marked for
	// deletion yet. Even in the remote case these blocks have already been marked for deletion,
	// the block.MarkForDeletion() call will correctly handle it.
	duplicateIDs := s.deduplicateBlocksFilter.DuplicateIDs()

	for _, id := range duplicateIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Spawn a new context so we always mark a block for deletion in full on shutdown.
		delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

		level.Info(s.logger).Log("msg", "marking outdated block for deletion", "block", id)
		err := block.MarkForDeletion(delCtx, s.logger, s.bkt, id, "outdated block", s.metrics.blocksMarkedForDeletion)
		cancel()
		if err != nil {
			s.metrics.garbageCollectionFailures.Inc()
			return errors.Wrapf(err, "mark block %s for deletion", id)
		}

		// Immediately update our in-memory state so no further call to SyncMetas is needed
		// after running garbage collection.
		delete(s.blocks, id)
	}
	s.metrics.garbageCollections.Inc()
	s.metrics.garbageCollectionDuration.Observe(time.Since(begin).Seconds())
	return nil
}

// Grouper is responsible to group all known blocks into compaction Job which are safe to be
// compacted concurrently.
type Grouper interface {
	// Groups returns the compaction jobs for all blocks currently known to the syncer.
	// It creates all jobs from the scratch on every call.
	Groups(blocks map[ulid.ULID]*block.Meta) (res []*Job, err error)
}

// DefaultGroupKey returns a unique identifier for the group the block belongs to, based on
// the DefaultGrouper logic. It considers the downsampling resolution and the block's labels.
func DefaultGroupKey(meta block.ThanosMeta) string {
	return defaultGroupKey(meta.Downsample.Resolution, labels.FromMap(meta.Labels))
}

func defaultGroupKey(res int64, lbls labels.Labels) string {
	return fmt.Sprintf("%d@%v", res, labels.StableHash(lbls))
}

func minTime(metas []*block.Meta) time.Time {
	if len(metas) == 0 {
		return time.Time{}
	}

	minT := metas[0].MinTime
	for _, meta := range metas {
		if meta.MinTime < minT {
			minT = meta.MinTime
		}
	}

	return time.Unix(0, minT*int64(time.Millisecond)).UTC()
}

func maxTime(metas []*block.Meta) time.Time {
	if len(metas) == 0 {
		return time.Time{}
	}

	maxT := metas[0].MaxTime
	for _, meta := range metas {
		if meta.MaxTime > maxT {
			maxT = meta.MaxTime
		}
	}

	return time.Unix(0, maxT*int64(time.Millisecond)).UTC()
}

// Planner returns blocks to compact.
type Planner interface {
	// Plan returns a list of blocks that should be compacted into single one.
	// The blocks can be overlapping. The provided metadata has to be ordered by minTime.
	Plan(ctx context.Context, metasByMinTime []*block.Meta) ([]*block.Meta, error)
}

// Compactor provides compaction against an underlying storage of time series data.
// This is similar to tsdb.Compactor just without Plan method.
// TODO(bwplotka): Split the Planner from Compactor on upstream as well, so we can import it.
type Compactor interface {
	// Write persists a Block into a directory.
	// No Block is written when resulting Block has 0 samples, and returns empty ulid.ULID{}.
	Write(dest string, b tsdb.BlockReader, mint, maxt int64, parent *tsdb.BlockMeta) (ulid.ULID, error)

	// Compact runs compaction against the provided directories. Must
	// only be called concurrently with results of Plan().
	// Can optionally pass a list of already open blocks,
	// to avoid having to reopen them.
	// When resulting Block has 0 samples
	//  * No block is written.
	//  * The source dirs are marked Deletable.
	//  * Returns empty ulid.ULID{}.
	Compact(dest string, dirs []string, open []*tsdb.Block) (ulid.ULID, error)

	// CompactWithSplitting merges and splits the input blocks into shardCount number of output blocks,
	// and returns slice of block IDs. Position of returned block ID in the result slice corresponds to the shard index.
	// If given output block has no series, corresponding block ID will be zero ULID value.
	CompactWithSplitting(dest string, dirs []string, open []*tsdb.Block, shardCount uint64) (result []ulid.ULID, _ error)
}

// runCompactionJob plans and runs a single compaction against the provided job. The compacted result
// is uploaded into the bucket the blocks were retrieved from.
func (c *BucketCompactor) runCompactionJob(ctx context.Context, job *Job) (shouldRerun bool, compIDs []ulid.ULID, rerr error) {
	jobBeginTime := time.Now()

	jobLogger := log.With(c.logger, "groupKey", job.Key())
	subDir := filepath.Join(c.compactDir, job.Key())

	defer func() {
		elapsed := time.Since(jobBeginTime)

		if rerr == nil {
			level.Info(jobLogger).Log("msg", "compaction job succeeded", "duration", elapsed, "duration_ms", elapsed.Milliseconds())
		} else {
			level.Error(jobLogger).Log("msg", "compaction job failed", "duration", elapsed, "duration_ms", elapsed.Milliseconds(), "err", rerr)
		}

		if err := os.RemoveAll(subDir); err != nil {
			level.Error(jobLogger).Log("msg", "failed to remove compaction group work directory", "path", subDir, "err", err)
		}
	}()

	if err := os.MkdirAll(subDir, 0750); err != nil {
		return false, nil, errors.Wrap(err, "create compaction job dir")
	}

	toCompact, err := c.planner.Plan(ctx, job.metasByMinTime)
	if err != nil {
		return false, nil, errors.Wrap(err, "plan compaction")
	}
	if len(toCompact) == 0 {
		// Nothing to do.
		return false, nil, nil
	}

	// The planner returned some blocks to compact, so we can enrich the logger
	// with the min/max time between all blocks to compact.
	jobLogger = log.With(jobLogger, "minTime", minTime(toCompact).String(), "maxTime", maxTime(toCompact).String())

	level.Info(jobLogger).Log("msg", "compaction available and planned; downloading blocks", "blocks", len(toCompact), "plan", fmt.Sprintf("%v", toCompact))

	// Once we have a plan we need to download the actual data.
	downloadBegin := time.Now()

	err = concurrency.ForEachJob(ctx, len(toCompact), c.blockSyncConcurrency, func(ctx context.Context, idx int) error {
		meta := toCompact[idx]

		// Must be the same as in blocksToCompactDirs.
		bdir := filepath.Join(subDir, meta.ULID.String())

		if err := block.Download(ctx, jobLogger, c.bkt, meta.ULID, bdir); err != nil {
			return errors.Wrapf(err, "download block %s", meta.ULID)
		}

		// Ensure all input blocks are valid.
		stats, err := block.GatherBlockHealthStats(jobLogger, bdir, meta.MinTime, meta.MaxTime, false)
		if err != nil {
			return errors.Wrapf(err, "gather index issues for block %s", bdir)
		}

		if err := stats.CriticalErr(); err != nil {
			return errors.Wrapf(err, "block with not healthy index found %s; Compaction level %v; Labels: %v", bdir, meta.Compaction.Level, meta.Thanos.Labels)
		}

		if err := stats.OutOfOrderChunksErr(); err != nil {
			return outOfOrderChunkError(errors.Wrapf(err, "blocks with out-of-order chunks are dropped from compaction:  %s", bdir), meta.ULID)
		}

		if err := stats.Issue347OutsideChunksErr(); err != nil {
			return issue347Error(errors.Wrapf(err, "invalid, but reparable block %s", bdir), meta.ULID)
		}

		if err := stats.OutOfOrderLabelsErr(); err != nil {
			return errors.Wrapf(err, "block id %s", meta.ULID)
		}
		return nil
	})
	if err != nil {
		return false, nil, err
	}

	blocksToCompactDirs := make([]string, len(toCompact))
	for ix, meta := range toCompact {
		blocksToCompactDirs[ix] = filepath.Join(subDir, meta.ULID.String())
	}

	elapsed := time.Since(downloadBegin)
	level.Info(jobLogger).Log("msg", "downloaded and verified blocks; compacting blocks", "blocks", len(blocksToCompactDirs), "plan", fmt.Sprintf("%v", blocksToCompactDirs), "duration", elapsed, "duration_ms", elapsed.Milliseconds())

	compactionBegin := time.Now()

	if job.UseSplitting() {
		compIDs, err = c.comp.CompactWithSplitting(subDir, blocksToCompactDirs, nil, uint64(job.SplittingShards()))
	} else {
		var compID ulid.ULID
		compID, err = c.comp.Compact(subDir, blocksToCompactDirs, nil)
		compIDs = append(compIDs, compID)
	}
	if err != nil {
		return false, nil, errors.Wrapf(err, "compact blocks %v", blocksToCompactDirs)
	}

	if !hasNonZeroULIDs(compIDs) {
		// Prometheus compactor found that the compacted block would have no samples.
		level.Info(jobLogger).Log("msg", "compacted block would have no samples, deleting source blocks", "blocks", fmt.Sprintf("%v", blocksToCompactDirs))
		for _, meta := range toCompact {
			if meta.Stats.NumSamples == 0 {
				if err := deleteBlock(c.bkt, meta.ULID, filepath.Join(subDir, meta.ULID.String()), jobLogger, c.metrics.blocksMarkedForDeletion); err != nil {
					level.Warn(jobLogger).Log("msg", "failed to mark for deletion an empty block found during compaction", "block", meta.ULID, "err", err)
				}
			}
		}
		// Even though this block was empty, there may be more work to do.
		return true, nil, nil
	}

	elapsed = time.Since(compactionBegin)
	level.Info(jobLogger).Log("msg", "compacted blocks", "new", fmt.Sprintf("%v", compIDs), "blocks", fmt.Sprintf("%v", blocksToCompactDirs), "duration", elapsed, "duration_ms", elapsed.Milliseconds())

	uploadBegin := time.Now()
	uploadedBlocks := atomic.NewInt64(0)

	blocksToUpload := convertCompactionResultToForEachJobs(compIDs, job.UseSplitting(), jobLogger)
	err = concurrency.ForEachJob(ctx, len(blocksToUpload), c.blockSyncConcurrency, func(ctx context.Context, idx int) error {
		blockToUpload := blocksToUpload[idx]

		uploadedBlocks.Inc()

		bdir := filepath.Join(subDir, blockToUpload.ulid.String())

		// When splitting is enabled, we need to inject the shard ID as external label.
		newLabels := job.Labels().Map()
		if job.UseSplitting() {
			newLabels[mimir_tsdb.CompactorShardIDExternalLabel] = sharding.FormatShardIDLabelValue(uint64(blockToUpload.shardIndex), uint64(job.SplittingShards()))
		}

		newMeta, err := block.InjectThanosMeta(jobLogger, bdir, block.ThanosMeta{
			Labels:       newLabels,
			Downsample:   block.ThanosDownsample{Resolution: job.Resolution()},
			Source:       block.CompactorSource,
			SegmentFiles: block.GetSegmentFiles(bdir),
		}, nil)
		if err != nil {
			return errors.Wrapf(err, "failed to finalize the block %s", bdir)
		}

		if err = os.Remove(filepath.Join(bdir, "tombstones")); err != nil {
			return errors.Wrap(err, "remove tombstones")
		}

		// Ensure the output block is valid.
		if err := block.VerifyBlock(jobLogger, bdir, newMeta.MinTime, newMeta.MaxTime, false); err != nil {
			return errors.Wrapf(err, "invalid result block %s", bdir)
		}

		begin := time.Now()
		if err := block.Upload(ctx, jobLogger, c.bkt, bdir, nil); err != nil {
			return errors.Wrapf(err, "upload of %s failed", blockToUpload.ulid)
		}

		elapsed := time.Since(begin)
		level.Info(jobLogger).Log("msg", "uploaded block", "result_block", blockToUpload.ulid, "duration", elapsed, "duration_ms", elapsed.Milliseconds(), "external_labels", labels.FromMap(newLabels))
		return nil
	})
	if err != nil {
		return false, nil, err
	}

	elapsed = time.Since(uploadBegin)
	level.Info(jobLogger).Log("msg", "uploaded all blocks", "blocks", uploadedBlocks, "duration", elapsed, "duration_ms", elapsed.Milliseconds())

	// Mark for deletion the blocks we just compacted from the job and bucket so they do not get included
	// into the next planning cycle.
	// Eventually the block we just uploaded should get synced into the job again (including sync-delay).
	for _, meta := range toCompact {
		if err := deleteBlock(c.bkt, meta.ULID, filepath.Join(subDir, meta.ULID.String()), jobLogger, c.metrics.blocksMarkedForDeletion); err != nil {
			return false, nil, errors.Wrapf(err, "mark old block for deletion from bucket")
		}
	}

	return true, compIDs, nil
}

// convertCompactionResultToForEachJobs filters out empty ULIDs.
// When handling result of split compactions, shard index is index in the slice returned by compaction.
func convertCompactionResultToForEachJobs(compactedBlocks []ulid.ULID, splitJob bool, jobLogger log.Logger) []ulidWithShardIndex {
	result := make([]ulidWithShardIndex, 0, len(compactedBlocks))

	for ix, id := range compactedBlocks {
		// Skip if it's an empty block.
		if id == (ulid.ULID{}) {
			if splitJob {
				level.Info(jobLogger).Log("msg", "compaction produced an empty block", "shard_id", sharding.FormatShardIDLabelValue(uint64(ix), uint64(len(compactedBlocks))))
			} else {
				level.Info(jobLogger).Log("msg", "compaction produced an empty block")
			}
			continue
		}

		result = append(result, ulidWithShardIndex{shardIndex: ix, ulid: id})
	}
	return result
}

type ulidWithShardIndex struct {
	ulid       ulid.ULID
	shardIndex int
}

// Issue347Error is a type wrapper for errors that should invoke repair process for broken block.
type Issue347Error struct {
	err error

	id ulid.ULID
}

func issue347Error(err error, brokenBlock ulid.ULID) Issue347Error {
	return Issue347Error{err: err, id: brokenBlock}
}

func (e Issue347Error) Error() string {
	return e.err.Error()
}

// IsIssue347Error returns true if the base error is a Issue347Error.
func IsIssue347Error(err error) bool {
	_, ok := errors.Cause(err).(Issue347Error)
	return ok
}

// OutOfOrderChunkError is a type wrapper for OOO chunk error from validating block index.
type OutOfOrderChunksError struct {
	err error
	id  ulid.ULID
}

func (e OutOfOrderChunksError) Error() string {
	return e.err.Error()
}

func outOfOrderChunkError(err error, brokenBlock ulid.ULID) OutOfOrderChunksError {
	return OutOfOrderChunksError{err: err, id: brokenBlock}
}

// IsOutOfOrderChunk returns true if the base error is a OutOfOrderChunkError.
func IsOutOfOrderChunkError(err error) bool {
	_, ok := errors.Cause(err).(OutOfOrderChunksError)
	return ok
}

// RepairIssue347 repairs the https://github.com/prometheus/tsdb/issues/347 issue when having issue347Error.
func RepairIssue347(ctx context.Context, logger log.Logger, bkt objstore.Bucket, blocksMarkedForDeletion prometheus.Counter, issue347Err error) error {
	ie, ok := errors.Cause(issue347Err).(Issue347Error)
	if !ok {
		return errors.Errorf("Given error is not an issue347 error: %v", issue347Err)
	}

	level.Info(logger).Log("msg", "Repairing block broken by https://github.com/prometheus/tsdb/issues/347", "id", ie.id, "err", issue347Err)

	tmpdir, err := os.MkdirTemp("", fmt.Sprintf("repair-issue-347-id-%s-", ie.id))
	if err != nil {
		return err
	}

	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			level.Warn(logger).Log("msg", "failed to remote tmpdir", "err", err, "tmpdir", tmpdir)
		}
	}()

	bdir := filepath.Join(tmpdir, ie.id.String())
	if err := block.Download(ctx, logger, bkt, ie.id, bdir); err != nil {
		return errors.Wrapf(err, "download block %s", ie.id)
	}

	meta, err := block.ReadMetaFromDir(bdir)
	if err != nil {
		return errors.Wrapf(err, "read meta from %s", bdir)
	}

	resid, err := block.Repair(logger, tmpdir, ie.id, block.CompactorRepairSource, block.IgnoreIssue347OutsideChunk)
	if err != nil {
		return errors.Wrapf(err, "repair failed for block %s", ie.id)
	}

	// Verify repaired id before uploading it.
	if err := block.VerifyBlock(logger, filepath.Join(tmpdir, resid.String()), meta.MinTime, meta.MaxTime, false); err != nil {
		return errors.Wrapf(err, "repaired block is invalid %s", resid)
	}

	level.Info(logger).Log("msg", "uploading repaired block", "newID", resid)
	if err = block.Upload(ctx, logger, bkt, filepath.Join(tmpdir, resid.String()), nil); err != nil {
		return errors.Wrapf(err, "upload of %s failed", resid)
	}

	level.Info(logger).Log("msg", "deleting broken block", "id", ie.id)

	// Spawn a new context so we always mark a block for deletion in full on shutdown.
	delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// TODO(bplotka): Issue with this will introduce overlap that will halt compactor. Automate that (fix duplicate overlaps caused by this).
	if err := block.MarkForDeletion(delCtx, logger, bkt, ie.id, "source of repaired block", blocksMarkedForDeletion); err != nil {
		return errors.Wrapf(err, "marking old block %s for deletion has failed", ie.id)
	}
	return nil
}

func deleteBlock(bkt objstore.Bucket, id ulid.ULID, bdir string, logger log.Logger, blocksMarkedForDeletion prometheus.Counter) error {
	if err := os.RemoveAll(bdir); err != nil {
		return errors.Wrapf(err, "remove old block dir %s", id)
	}

	// Spawn a new context so we always mark a block for deletion in full on shutdown.
	delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	level.Info(logger).Log("msg", "marking compacted block for deletion", "old_block", id)
	if err := block.MarkForDeletion(delCtx, logger, bkt, id, "source of compacted block", blocksMarkedForDeletion); err != nil {
		return errors.Wrapf(err, "mark block %s for deletion from bucket", id)
	}
	return nil
}

// BucketCompactorMetrics holds the metrics tracked by BucketCompactor.
type BucketCompactorMetrics struct {
	groupCompactionRunsStarted   prometheus.Counter
	groupCompactionRunsCompleted prometheus.Counter
	groupCompactionRunsFailed    prometheus.Counter
	groupCompactions             prometheus.Counter
	blocksMarkedForDeletion      prometheus.Counter
	blocksMarkedForNoCompact     prometheus.Counter
	blocksMaxTimeDelta           prometheus.Histogram
}

// NewBucketCompactorMetrics makes a new BucketCompactorMetrics.
func NewBucketCompactorMetrics(blocksMarkedForDeletion prometheus.Counter, reg prometheus.Registerer) *BucketCompactorMetrics {
	return &BucketCompactorMetrics{
		groupCompactionRunsStarted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_group_compaction_runs_started_total",
			Help: "Total number of group compaction attempts.",
		}),
		groupCompactionRunsCompleted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_group_compaction_runs_completed_total",
			Help: "Total number of group completed compaction runs. This also includes compactor group runs that resulted with no compaction.",
		}),
		groupCompactionRunsFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_group_compactions_failures_total",
			Help: "Total number of failed group compactions.",
		}),
		groupCompactions: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_group_compactions_total",
			Help: "Total number of group compaction attempts that resulted in new block(s).",
		}),
		blocksMarkedForDeletion: blocksMarkedForDeletion,
		blocksMarkedForNoCompact: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_compactor_blocks_marked_for_no_compaction_total",
			Help:        "Total number of blocks that were marked for no-compaction.",
			ConstLabels: prometheus.Labels{"reason": block.OutOfOrderChunksNoCompactReason},
		}),
		blocksMaxTimeDelta: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_compactor_block_max_time_delta_seconds",
			Help:    "Difference between now and the max time of a block being compacted in seconds.",
			Buckets: prometheus.LinearBuckets(86400, 43200, 8), // 1 to 5 days, in 12 hour intervals
		}),
	}
}

type ownCompactionJobFunc func(job *Job) (bool, error)

// ownAllJobs is a ownCompactionJobFunc that always return true.
var ownAllJobs = func(job *Job) (bool, error) {
	return true, nil
}

// BucketCompactor compacts blocks in a bucket.
type BucketCompactor struct {
	logger                         log.Logger
	sy                             *Syncer
	grouper                        Grouper
	comp                           Compactor
	planner                        Planner
	compactDir                     string
	bkt                            objstore.Bucket
	concurrency                    int
	skipBlocksWithOutOfOrderChunks bool
	ownJob                         ownCompactionJobFunc
	sortJobs                       JobsOrderFunc
	waitPeriod                     time.Duration
	blockSyncConcurrency           int
	metrics                        *BucketCompactorMetrics
}

// NewBucketCompactor creates a new bucket compactor.
func NewBucketCompactor(
	logger log.Logger,
	sy *Syncer,
	grouper Grouper,
	planner Planner,
	comp Compactor,
	compactDir string,
	bkt objstore.Bucket,
	concurrency int,
	skipBlocksWithOutOfOrderChunks bool,
	ownJob ownCompactionJobFunc,
	sortJobs JobsOrderFunc,
	waitPeriod time.Duration,
	blockSyncConcurrency int,
	metrics *BucketCompactorMetrics,
) (*BucketCompactor, error) {
	if concurrency <= 0 {
		return nil, errors.Errorf("invalid concurrency level (%d), concurrency level must be > 0", concurrency)
	}
	return &BucketCompactor{
		logger:                         logger,
		sy:                             sy,
		grouper:                        grouper,
		planner:                        planner,
		comp:                           comp,
		compactDir:                     compactDir,
		bkt:                            bkt,
		concurrency:                    concurrency,
		skipBlocksWithOutOfOrderChunks: skipBlocksWithOutOfOrderChunks,
		ownJob:                         ownJob,
		sortJobs:                       sortJobs,
		waitPeriod:                     waitPeriod,
		blockSyncConcurrency:           blockSyncConcurrency,
		metrics:                        metrics,
	}, nil
}

// Compact runs compaction over bucket.
// If maxCompactionTime is positive then after this time no more new compactions are started.
func (c *BucketCompactor) Compact(ctx context.Context, maxCompactionTime time.Duration) (rerr error) {
	defer func() {
		// Do not remove the compactDir if an error has occurred
		// because potentially on the next run we would not have to download
		// everything again.
		if rerr != nil {
			return
		}
		if err := os.RemoveAll(c.compactDir); err != nil {
			level.Error(c.logger).Log("msg", "failed to remove compaction work directory", "path", c.compactDir, "err", err)
		}
	}()

	var maxCompactionTimeChan <-chan time.Time
	if maxCompactionTime > 0 {
		maxCompactionTimeChan = time.After(maxCompactionTime)
	}

	// Loop over bucket and compact until there's no work left.
	for {
		var (
			wg                     sync.WaitGroup
			workCtx, workCtxCancel = context.WithCancel(ctx)
			jobChan                = make(chan *Job)
			errChan                = make(chan error, c.concurrency)
			finishedAllJobs        = true
			mtx                    sync.Mutex
		)
		defer workCtxCancel()

		// Set up workers who will compact the jobs when the jobs are ready.
		// They will compact available jobs until they encounter an error, after which they will stop.
		for i := 0; i < c.concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for g := range jobChan {
					// Ensure the job is still owned by the current compactor instance.
					// If not, we shouldn't run it because another compactor instance may already
					// process it (or will do it soon).
					if ok, err := c.ownJob(g); err != nil {
						level.Info(c.logger).Log("msg", "skipped compaction because unable to check whether the job is owned by the compactor instance", "groupKey", g.Key(), "err", err)
						continue
					} else if !ok {
						level.Info(c.logger).Log("msg", "skipped compaction because job is not owned by the compactor instance anymore", "groupKey", g.Key())
						continue
					}

					c.metrics.groupCompactionRunsStarted.Inc()

					shouldRerunJob, compactedBlockIDs, err := c.runCompactionJob(workCtx, g)
					if err == nil {
						c.metrics.groupCompactionRunsCompleted.Inc()
						if hasNonZeroULIDs(compactedBlockIDs) {
							c.metrics.groupCompactions.Inc()
						}

						if shouldRerunJob {
							mtx.Lock()
							finishedAllJobs = false
							mtx.Unlock()
						}
						continue
					}

					// At this point the compaction has failed.
					c.metrics.groupCompactionRunsFailed.Inc()

					if IsIssue347Error(err) {
						if err := RepairIssue347(workCtx, c.logger, c.bkt, c.sy.metrics.blocksMarkedForDeletion, err); err == nil {
							mtx.Lock()
							finishedAllJobs = false
							mtx.Unlock()
							continue
						}
					}
					// If block has out of order chunk and it has been configured to skip it,
					// then we can mark the block for no compaction so that the next compaction run
					// will skip it.
					if IsOutOfOrderChunkError(err) && c.skipBlocksWithOutOfOrderChunks {
						if err := block.MarkForNoCompact(
							ctx,
							c.logger,
							c.bkt,
							err.(OutOfOrderChunksError).id,
							block.OutOfOrderChunksNoCompactReason,
							"OutofOrderChunk: marking block with out-of-order series/chunks to as no compact to unblock compaction", c.metrics.blocksMarkedForNoCompact); err == nil {
							mtx.Lock()
							finishedAllJobs = false
							mtx.Unlock()
							continue
						}
					}
					errChan <- errors.Wrapf(err, "group %s", g.Key())
					return
				}
			}()
		}

		level.Info(c.logger).Log("msg", "start sync of metas")
		if err := c.sy.SyncMetas(ctx); err != nil {
			return errors.Wrap(err, "sync")
		}

		level.Info(c.logger).Log("msg", "start of GC")
		// Blocks that were compacted are garbage collected after each Compaction.
		// However if compactor crashes we need to resolve those on startup.
		if err := c.sy.GarbageCollect(ctx); err != nil {
			return errors.Wrap(err, "blocks garbage collect")
		}

		jobs, err := c.grouper.Groups(c.sy.Metas())
		if err != nil {
			return errors.Wrap(err, "build compaction jobs")
		}

		// There is another check just before we start processing the job, but we can avoid sending it
		// to the goroutine in the first place.
		jobs, err = c.filterOwnJobs(jobs)
		if err != nil {
			return err
		}

		// Record the difference between now and the max time for a block being compacted. This
		// is used to detect compactors not being able to keep up with the rate of blocks being
		// created. The idea is that most blocks should be for within 24h or 48h.
		now := time.Now()
		for _, delta := range c.blockMaxTimeDeltas(now, jobs) {
			c.metrics.blocksMaxTimeDelta.Observe(delta)
		}

		// Skip jobs for which the wait period hasn't been honored yet.
		jobs = c.filterJobsByWaitPeriod(ctx, jobs)

		// Sort jobs based on the configured ordering algorithm.
		jobs = c.sortJobs(jobs)

		ignoreDirs := []string{}
		for _, gr := range jobs {
			for _, grID := range gr.IDs() {
				ignoreDirs = append(ignoreDirs, filepath.Join(gr.Key(), grID.String()))
			}
		}

		if err := runutil.DeleteAll(c.compactDir, ignoreDirs...); err != nil {
			level.Warn(c.logger).Log("msg", "failed deleting non-compaction job directories/files, some disk space usage might have leaked. Continuing", "err", err, "dir", c.compactDir)
		}

		level.Info(c.logger).Log("msg", "start of compactions")

		maxCompactionTimeReached := false
		// Send all jobs found during this pass to the compaction workers.
		var jobErrs multierror.MultiError
	jobLoop:
		for _, g := range jobs {
			select {
			case jobErr := <-errChan:
				jobErrs.Add(jobErr)
				break jobLoop
			case jobChan <- g:
			case <-maxCompactionTimeChan:
				maxCompactionTimeReached = true
				level.Info(c.logger).Log("msg", "max compaction time reached, no more compactions will be started")
				break jobLoop
			}
		}
		close(jobChan)
		wg.Wait()

		// Collect any other error reported by the workers, or any error reported
		// while we were waiting for the last batch of jobs to run the compaction.
		close(errChan)
		for jobErr := range errChan {
			jobErrs.Add(jobErr)
		}

		workCtxCancel()
		if len(jobErrs) > 0 {
			return jobErrs.Err()
		}

		if maxCompactionTimeReached || finishedAllJobs {
			break
		}
	}
	level.Info(c.logger).Log("msg", "compaction iterations done")
	return nil
}

// blockMaxTimeDeltas returns a slice of the difference between now and the MaxTime of each
// block that will be compacted as part of the provided jobs, in seconds.
func (c *BucketCompactor) blockMaxTimeDeltas(now time.Time, jobs []*Job) []float64 {
	var out []float64

	for _, j := range jobs {
		for _, m := range j.Metas() {
			out = append(out, now.Sub(time.UnixMilli(m.MaxTime)).Seconds())
		}
	}

	return out
}

func (c *BucketCompactor) filterOwnJobs(jobs []*Job) ([]*Job, error) {
	for ix := 0; ix < len(jobs); {
		// Skip any job which doesn't belong to this compactor instance.
		if ok, err := c.ownJob(jobs[ix]); err != nil {
			return nil, errors.Wrap(err, "ownJob")
		} else if !ok {
			jobs = append(jobs[:ix], jobs[ix+1:]...)
		} else {
			ix++
		}
	}
	return jobs, nil
}

// filterJobsByWaitPeriod filters out jobs for which the configured wait period hasn't been honored yet.
func (c *BucketCompactor) filterJobsByWaitPeriod(ctx context.Context, jobs []*Job) []*Job {
	for i := 0; i < len(jobs); {
		if elapsed, notElapsedBlock, err := jobWaitPeriodElapsed(ctx, jobs[i], c.waitPeriod, c.bkt); err != nil {
			level.Warn(c.logger).Log("msg", "not enforcing compaction wait period because the check if compaction job contains recently uploaded blocks has failed", "groupKey", jobs[i].Key(), "err", err)

			// Keep the job.
			i++
		} else if !elapsed {
			level.Info(c.logger).Log("msg", "skipping compaction job because blocks in this job were uploaded too recently (within wait period)", "groupKey", jobs[i].Key(), "waitPeriodNotElapsedFor", notElapsedBlock.String())
			jobs = append(jobs[:i], jobs[i+1:]...)
		} else {
			i++
		}
	}

	return jobs
}

var _ block.MetadataFilter = &NoCompactionMarkFilter{}

// NoCompactionMarkFilter is a block.Fetcher filter that finds all blocks with no-compact marker files, and optionally
// removes them from synced metas.
type NoCompactionMarkFilter struct {
	bkt                   objstore.InstrumentedBucketReader
	noCompactMarkedMap    map[ulid.ULID]struct{}
	removeNoCompactBlocks bool
}

// NewNoCompactionMarkFilter creates NoCompactionMarkFilter.
func NewNoCompactionMarkFilter(bkt objstore.InstrumentedBucketReader, removeNoCompactBlocks bool) *NoCompactionMarkFilter {
	return &NoCompactionMarkFilter{
		bkt:                   bkt,
		removeNoCompactBlocks: removeNoCompactBlocks,
	}
}

// NoCompactMarkedBlocks returns block ids that were marked for no compaction.
// It is safe to call this method only after Filter has finished, and it is also safe to manipulate the map between calls to Filter.
func (f *NoCompactionMarkFilter) NoCompactMarkedBlocks() map[ulid.ULID]struct{} {
	return f.noCompactMarkedMap
}

// Filter finds blocks that should not be compacted, and fills f.noCompactMarkedMap. If f.removeNoCompactBlocks is true,
// blocks are also removed from metas. (Thanos version of the filter doesn't do removal).
func (f *NoCompactionMarkFilter) Filter(ctx context.Context, metas map[ulid.ULID]*block.Meta, synced block.GaugeVec) error {
	noCompactMarkedMap := make(map[ulid.ULID]struct{})

	// Find all no-compact markers in the storage.
	err := f.bkt.Iter(ctx, block.MarkersPathname+"/", func(name string) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		if blockID, ok := block.IsNoCompactMarkFilename(path.Base(name)); ok {
			_, exists := metas[blockID]
			if exists {
				noCompactMarkedMap[blockID] = struct{}{}
				synced.WithLabelValues(block.MarkedForNoCompactionMeta).Inc()

				if f.removeNoCompactBlocks {
					delete(metas, blockID)
				}
			}

		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "list block no-compact marks")
	}

	f.noCompactMarkedMap = noCompactMarkedMap
	return nil
}

func hasNonZeroULIDs(ids []ulid.ULID) bool {
	for _, id := range ids {
		if id != (ulid.ULID{}) {
			return true
		}
	}

	return false
}
