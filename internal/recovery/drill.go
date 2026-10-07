package recovery

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	stationclock "orbitlink/internal/clock"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/rf"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

// keptReports bounds the in-memory drill history.
const keptReports = 16

// snapshotComponents are the durable documents a restart is expected to find.
var snapshotComponents = []string{
	"satellites", "ephemerides", "clock", "passes", "pass-windows",
	"antenna-timeline", "pointing-commands", "antennas", "rf-chains",
	"recording-sessions", "receiver-attempts",
}

// Runner executes recovery drills against a read-only copy of the live state
// root and keeps the recent reports in memory. It never opens a live snapshot
// for writing: the only filesystem access to the live root is reading.
type Runner struct {
	mu        sync.Mutex
	stateRoot string
	reports   []Report
}

func NewRunner(stateRoot string) *Runner {
	return &Runner{stateRoot: stateRoot}
}

// List returns stored reports, newest first.
func (r *Runner) List() []Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Report(nil), r.reports...)
}

func (r *Runner) Get(id string) (Report, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, report := range r.reports {
		if report.ID == id {
			return report, true
		}
	}
	return Report{}, false
}

// Run copies the live snapshots into a temporary sandbox, rebuilds every
// component there exactly as a restarted process would, replays each domain
// and simulates its recovery actions. Live state is hashed before and after
// so the report can prove the drill left queues and device snapshots intact.
func (r *Runner) Run(ctx context.Context) (Report, error) {
	before, err := hashSnapshots(r.stateRoot)
	if err != nil {
		return Report{}, err
	}
	sandbox, err := os.MkdirTemp("", "orbitlink-recovery-*")
	if err != nil {
		return Report{}, fmt.Errorf("create drill sandbox: %w", err)
	}
	defer func() { _ = os.RemoveAll(sandbox) }()

	copied, err := copySnapshots(r.stateRoot, sandbox)
	if err != nil {
		return Report{}, err
	}
	env, err := loadSandbox(sandbox)
	if err != nil {
		return Report{}, err
	}

	report := Report{
		ID:         uuid.NewString(),
		StartedAt:  time.Now().UTC(),
		StateRoot:  r.stateRoot,
		RebootedAt: env.clock.Now(),
	}
	drill := newDrill(ctx, env)
	report.Domains = []DomainReplay{
		drill.replayPasses(),
		drill.replayReservations(),
		drill.replayAntennas(),
		drill.replayRFChains(),
		drill.replayRecordings(),
	}
	report.Missing = missingComponents(copied, env)

	after, err := hashSnapshots(r.stateRoot)
	if err != nil {
		return Report{}, err
	}
	report.LiveFilesVerified = len(after)
	report.LiveStateUnchanged = equalHashes(before, after)
	if !report.LiveStateUnchanged {
		report.Risks = append(report.Risks, Risk{
			Subject:  "live-state",
			Severity: RiskCritical,
			Detail:   "live snapshots changed while the drill ran; drill writes go to a sandbox, so this indicates concurrent live traffic",
		})
	}
	report.FinishedAt = time.Now().UTC()
	report.summarize()

	r.mu.Lock()
	r.reports = append([]Report{report}, r.reports...)
	if len(r.reports) > keptReports {
		r.reports = r.reports[:keptReports]
	}
	r.mu.Unlock()
	return report, nil
}

// sandboxEnv holds the components rebuilt from the read-only copy. Every
// write they perform lands in the sandbox, mirroring a real process restart.
type sandboxEnv struct {
	clock        *stationclock.Service
	windows      *store.WindowStore
	timeline     *scheduler.Timeline
	commands     *scheduler.CommandQueue
	fleet        *antenna.Fleet
	rf           *rf.Controller
	recorder     *recorder.Manager
	passes       *passsvc.Service
	attempts     *receiver.AttemptDispatcher
	reservations *scheduler.ReservationPlanner
	replanner    *passsvc.Replanner
	recompute    *passsvc.WindowRecomputer
	finalizer    *passsvc.Finalizer
}

func loadSandbox(root string) (*sandboxEnv, error) {
	paths := store.NewPaths(root)
	trail := audit.NewTrail(store.NewAppendLog(paths))
	clockService, err := stationclock.NewService(paths, trail)
	if err != nil {
		return nil, err
	}
	windows, err := store.NewWindowStore(paths)
	if err != nil {
		return nil, err
	}
	timeline, err := scheduler.NewTimeline(paths, trail)
	if err != nil {
		return nil, err
	}
	commands, err := scheduler.NewCommandQueue(paths)
	if err != nil {
		return nil, err
	}
	fleet, err := antenna.NewFleet(paths, timeline, trail, nil)
	if err != nil {
		return nil, err
	}
	rfController, err := rf.NewController(paths, trail, nil)
	if err != nil {
		return nil, err
	}
	recorderManager, err := recorder.NewManager(paths, trail)
	if err != nil {
		return nil, err
	}
	passes, err := passsvc.NewService(paths, recorderManager, trail)
	if err != nil {
		return nil, err
	}
	attempts, err := receiver.NewAttemptDispatcher(paths, trail)
	if err != nil {
		return nil, err
	}
	recompute := passsvc.NewWindowRecomputer(windows, trail)
	return &sandboxEnv{
		clock:        clockService,
		windows:      windows,
		timeline:     timeline,
		commands:     commands,
		fleet:        fleet,
		rf:           rfController,
		recorder:     recorderManager,
		passes:       passes,
		attempts:     attempts,
		reservations: scheduler.NewReservationPlanner(windows, timeline, 30*time.Second),
		replanner:    passsvc.NewReplanner(passes, recompute, commands, trail),
		recompute:    recompute,
		finalizer:    passsvc.NewFinalizer(passes, recorderManager, receiver.NewUnlocker(trail), trail, 250*time.Millisecond),
	}, nil
}

// copySnapshots duplicates the live snapshot documents into the sandbox. The
// live files are opened read-only; the returned set names the components that
// had a snapshot to replay.
func copySnapshots(liveRoot, sandbox string) (map[string]bool, error) {
	copied := make(map[string]bool)
	entries, err := os.ReadDir(filepath.Join(liveRoot, "snapshots"))
	if os.IsNotExist(err) {
		return copied, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list live snapshots: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(sandbox, "snapshots"), 0o755); err != nil {
		return nil, fmt.Errorf("create sandbox snapshots: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(liveRoot, "snapshots", name))
		if err != nil {
			return nil, fmt.Errorf("read live snapshot %s: %w", name, err)
		}
		if err := os.WriteFile(filepath.Join(sandbox, "snapshots", name), data, 0o644); err != nil {
			return nil, fmt.Errorf("copy snapshot %s: %w", name, err)
		}
		copied[strings.TrimSuffix(name, ".json")] = true
	}
	return copied, nil
}

// hashSnapshots fingerprints every live snapshot so the drill can prove it
// did not rewrite queues or device snapshots.
func hashSnapshots(root string) (map[string]string, error) {
	hashes := make(map[string]string)
	entries, err := os.ReadDir(filepath.Join(root, "snapshots"))
	if os.IsNotExist(err) {
		return hashes, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list live snapshots: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, "snapshots", entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("hash live snapshot %s: %w", entry.Name(), err)
		}
		hashes[entry.Name()] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	return hashes, nil
}

func equalHashes(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for name, hash := range a {
		if b[name] != hash {
			return false
		}
	}
	return true
}

// missingComponents reports snapshot files the restart could not find and the
// runtime components that have no durable state at all.
func missingComponents(copied map[string]bool, env *sandboxEnv) []MissingComponent {
	var missing []MissingComponent
	for _, component := range snapshotComponents {
		if !copied[component] {
			missing = append(missing, MissingComponent{
				Component: component,
				Kind:      MissingSnapshotFile,
				Detail:    fmt.Sprintf("snapshots/%s.json is absent; the component restarts empty", component),
			})
		}
	}
	buffered := 0
	segments := 0
	for _, segment := range env.recorder.List() {
		if segment.FramesBuffered > 0 {
			segments++
			buffered += segment.FramesBuffered
		}
	}
	missing = append(missing,
		MissingComponent{Component: "pass-deadlines", Kind: MissingPersistenceGap,
			Detail: "monotonic pass deadlines are memory-only; tracking passes fall back to civil LOS after a restart"},
		MissingComponent{Component: "handover-sessions", Kind: MissingPersistenceGap,
			Detail: "in-flight recording ownership transfers are memory-only and vanish on restart"},
		MissingComponent{Component: "decoder-sessions", Kind: MissingPersistenceGap,
			Detail: "receiver and decoder session state is memory-only; symbol streams must resynchronize"},
		MissingComponent{Component: "recorder-buffer", Kind: MissingPersistenceGap,
			Detail: fmt.Sprintf("buffered frames are memory-only; %d segments currently hold %d undrained frames", segments, buffered)},
	)
	return missing
}

func terminal(phase passsvc.Phase) bool {
	switch phase {
	case passsvc.Completed, passsvc.Sealed, passsvc.Failed, passsvc.Canceled:
		return true
	}
	return false
}

func sortedPasses(values []passsvc.Pass) []passsvc.Pass {
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
	return values
}

func sortedSegments(values []recorder.Segment) []recorder.Segment {
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
	return values
}

func sortedAntennas(values []antenna.State) []antenna.State {
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
	return values
}

func sortedChains(values []rf.State) []rf.State {
	sort.Slice(values, func(i, j int) bool { return values[i].ChainID < values[j].ChainID })
	return values
}
