package recovery

import (
	"fmt"
	"sort"
	"time"

	"orbitlink/internal/antenna"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/rf"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

// snapshot names, matching the document each live manager persists through
// store.Paths.Snapshot. The drill only ever reads these files.
const (
	snapPasses     = "passes"
	snapWindows    = "pass-windows"
	snapTimeline   = "antenna-timeline"
	snapCommands   = "pointing-commands"
	snapAntennas   = "antennas"
	snapRFChains   = "rf-chains"
	snapRecordings = "recording-sessions"
	snapAttempts   = "receiver-attempts"
)

// Drill replays durable snapshots as if the process had just restarted. It holds
// no live manager references and performs no writes: every input is a detached
// copy decoded straight from disk, so the realtime queue and device snapshots
// cannot be mutated.
type Drill struct {
	paths store.Paths
}

func NewDrill(paths store.Paths) *Drill {
	return &Drill{paths: paths}
}

type replayState struct {
	now       time.Time
	passes    []passsvc.Pass
	windows   []store.WindowRecord
	intervals []scheduler.Interval
	commands  []scheduler.PointingCommand
	antennas  []antenna.State
	chains    []rf.State
	segments  []recorder.Segment
	attempts  []receiver.Attempt
}

// Run decodes every durable document once and derives the restart replay.
func (d *Drill) Run(now time.Time) (Report, error) {
	state := replayState{now: now.UTC()}
	loaders := []struct {
		name string
		load func() error
	}{
		{snapPasses, func() error { return d.read(snapPasses, &state.passes) }},
		{snapWindows, func() error { return d.read(snapWindows, &state.windows) }},
		{snapTimeline, func() error { return d.read(snapTimeline, &state.intervals) }},
		{snapCommands, func() error { return d.read(snapCommands, &state.commands) }},
		{snapAntennas, func() error { return d.read(snapAntennas, &state.antennas) }},
		{snapRFChains, func() error { return d.read(snapRFChains, &state.chains) }},
		{snapRecordings, func() error { return d.read(snapRecordings, &state.segments) }},
		{snapAttempts, func() error { return d.read(snapAttempts, &state.attempts) }},
	}
	components := make([]ComponentReplay, 0, len(loaders))
	for _, loader := range loaders {
		if err := loader.load(); err != nil {
			return Report{}, fmt.Errorf("replay %s snapshot: %w", loader.name, err)
		}
		components = append(components, ComponentReplay{Component: loader.name, Replayed: countReplayed(loader.name, &state)})
	}

	report := Report{
		GeneratedAt:        state.now,
		SimulatedRestartAt: state.now,
		StateRoot:          d.paths.Root,
		ReadOnly:           true,
		Components:         components,
	}

	index := buildIndex(&state)
	report.Passes = state.replayPasses(index)
	report.GlobalFindings = state.replayGlobals(index)
	report.MissingComponents = state.replayMissing(index)

	report.Summary = state.summarize(index, report.Passes, report.GlobalFindings, report.MissingComponents)
	return report, nil
}

func (d *Drill) read(snapshot string, target any) error {
	// A missing document simply means that component never persisted state;
	// ReadJSON reports found=false and leaves the target at its zero value.
	_, err := store.ReadJSON(d.paths.Snapshot(snapshot), target)
	return err
}

func countReplayed(name string, state *replayState) int {
	switch name {
	case snapPasses:
		return len(state.passes)
	case snapWindows:
		return len(state.windows)
	case snapTimeline:
		return len(state.intervals)
	case snapCommands:
		return len(state.commands)
	case snapAntennas:
		return len(state.antennas)
	case snapRFChains:
		return len(state.chains)
	case snapRecordings:
		return len(state.segments)
	case snapAttempts:
		return len(state.attempts)
	default:
		return 0
	}
}

type index struct {
	intervalByID    map[string]scheduler.Interval
	intervalsByPass map[string][]scheduler.Interval
	antennaByID     map[string]antenna.State
	chainByID       map[string]rf.State
	segmentByID     map[string]recorder.Segment
	attemptByID     map[string]receiver.Attempt
	currentAttempt  map[string]receiver.Attempt
	activeRev       map[string]int64
}

func buildIndex(state *replayState) *index {
	idx := &index{
		intervalByID:    make(map[string]scheduler.Interval),
		intervalsByPass: make(map[string][]scheduler.Interval),
		antennaByID:     make(map[string]antenna.State),
		chainByID:       make(map[string]rf.State),
		segmentByID:     make(map[string]recorder.Segment),
		attemptByID:     make(map[string]receiver.Attempt),
		currentAttempt:  make(map[string]receiver.Attempt),
		activeRev:       make(map[string]int64),
	}
	for _, interval := range state.intervals {
		idx.intervalByID[interval.ReservationID] = interval
		idx.intervalsByPass[interval.PassID] = append(idx.intervalsByPass[interval.PassID], interval)
	}
	for _, value := range state.antennas {
		idx.antennaByID[value.ID] = value
	}
	for _, value := range state.chains {
		idx.chainByID[value.ChainID] = value
	}
	for _, value := range state.segments {
		idx.segmentByID[value.ID] = value
	}
	// Mirror scheduler.CommandQueue: the active revision is the highest
	// non-retired revision for the pass.
	for _, command := range state.commands {
		if !command.Retired && command.Revision > idx.activeRev[command.PassID] {
			idx.activeRev[command.PassID] = command.Revision
		}
	}
	for _, attempt := range state.attempts {
		idx.attemptByID[attempt.ID] = attempt
		current := idx.currentAttempt[attempt.PassID]
		if attempt.Number >= current.Number {
			idx.currentAttempt[attempt.PassID] = attempt
		}
	}
	return idx
}

func (s *replayState) dueCommands(passID string, idx *index) []scheduler.PointingCommand {
	rev := idx.activeRev[passID]
	var due []scheduler.PointingCommand
	for _, command := range s.commands {
		if command.PassID == passID && !command.Retired && command.Revision == rev && !command.ExecuteAt.After(s.now) {
			due = append(due, command)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].ExecuteAt.Before(due[j].ExecuteAt) })
	return due
}

func liveIntervalExists(passID string, idx *index) bool {
	for _, interval := range idx.intervalsByPass[passID] {
		if interval.Status != scheduler.Released {
			return true
		}
	}
	return false
}

func hasCritical(findings []Finding) bool {
	for _, finding := range findings {
		if finding.Severity == SeverityCritical {
			return true
		}
	}
	return false
}

func finding(severity Severity, component, code, subject, message string) Finding {
	return Finding{Severity: severity, Component: component, Code: code, Subject: subject, Message: message}
}
