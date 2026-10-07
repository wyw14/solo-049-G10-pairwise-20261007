package recovery

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"orbitlink/internal/antenna"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/rf"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

type fixture struct {
	paths    store.Paths
	now      time.Time
	passes   []passsvc.Pass
	windows  []store.WindowRecord
	interval []scheduler.Interval
	commands []scheduler.PointingCommand
	antennas []antenna.State
	chains   []rf.State
	segments []recorder.Segment
	attempts []receiver.Attempt
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return &fixture{paths: store.NewPaths(t.TempDir()), now: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)}
}

func (f *fixture) write(t *testing.T) {
	t.Helper()
	pairs := []struct {
		name   string
		values any
	}{
		{snapPasses, f.passes}, {snapWindows, f.windows}, {snapTimeline, f.interval},
		{snapCommands, f.commands}, {snapAntennas, f.antennas}, {snapRFChains, f.chains},
		{snapRecordings, f.segments}, {snapAttempts, f.attempts},
	}
	for _, pair := range pairs {
		if err := store.WriteJSON(f.paths.Snapshot(pair.name), pair.values); err != nil {
			t.Fatalf("write %s: %v", pair.name, err)
		}
	}
}

func (f *fixture) run(t *testing.T) Report {
	t.Helper()
	report, err := NewDrill(f.paths).Run(f.now)
	if err != nil {
		t.Fatalf("drill run: %v", err)
	}
	return report
}

func actionKinds(pr PassRecovery) map[ActionKind]bool {
	kinds := make(map[ActionKind]bool)
	for _, action := range pr.Actions {
		kinds[action.Kind] = true
	}
	return kinds
}

func findingCodes(pr PassRecovery) map[string]Severity {
	codes := make(map[string]Severity)
	for _, finding := range pr.Findings {
		codes[finding.Code] = finding.Severity
	}
	return codes
}

func TestTrackingPassReplayInfersRecoverableActions(t *testing.T) {
	f := newFixture(t)
	aos, los := f.now.Add(-2*time.Minute), f.now.Add(6*time.Minute)
	passID, segID, resID := "PASS-TRK", "SEG-1", "RES-1"
	f.passes = []passsvc.Pass{{
		ID: passID, SatelliteID: "SAT-A12", AntennaID: "ANT-01",
		RFChains: []string{"RF-01", "RF-02"}, RequiredReceiverChains: []string{"RF-01", "RF-02"},
		Window: passsvc.Window{AOS: aos, LOS: los, Revision: 1}, Phase: passsvc.Tracking,
		ReservationID: resID, ReservationGeneration: 1, RecordingSegmentID: segID,
		OwnerStation: "north", OwnerGeneration: 1,
	}}
	f.interval = []scheduler.Interval{{ReservationID: resID, PassID: passID, AntennaID: "ANT-01", Start: aos.Add(-time.Minute), End: los, Generation: 1, Status: scheduler.Active}}
	f.antennas = []antenna.State{{ID: "ANT-01", Mode: antenna.Tracking, OwnerPassID: passID, OwnerGeneration: 1}}
	f.chains = []rf.State{{ChainID: "RF-01", PassID: passID, CenterHz: 2245e6}, {ChainID: "RF-02", PassID: passID, CenterHz: 2245e6}}
	f.segments = []recorder.Segment{{ID: segID, PassID: passID, OwnerStation: "north", OwnerGeneration: 1, Status: recorder.Open}}
	f.attempts = []receiver.Attempt{{ID: "ATT-1", PassID: passID, ChainID: "RF-01", Number: 1, Status: receiver.AttemptLocked, Epoch: 3}}
	f.commands = []scheduler.PointingCommand{
		{ID: "CMD-1", PassID: passID, AntennaID: "ANT-01", Revision: 1, ExecuteAt: f.now.Add(-time.Minute), Azimuth: 120, Elevation: 20},
		{ID: "CMD-2", PassID: passID, AntennaID: "ANT-01", Revision: 1, ExecuteAt: f.now.Add(time.Minute), Azimuth: 130, Elevation: 22},
	}
	f.write(t)

	report := f.run(t)
	if !report.ReadOnly {
		t.Fatal("report must declare itself read-only")
	}
	if len(report.Passes) != 1 {
		t.Fatalf("expected 1 pass replay, got %d", len(report.Passes))
	}
	pr := report.Passes[0]
	if !pr.Recoverable {
		t.Fatalf("tracking pass with intact durable state should be recoverable, findings=%v", pr.Findings)
	}
	kinds := actionKinds(pr)
	for _, want := range []ActionKind{ActionRebuildDeadline, ActionReplayDueCommands, ActionReacquireLock, ActionReopenDecoder, ActionResumeDrain} {
		if !kinds[want] {
			t.Errorf("expected recoverable action %s, got %v", want, kinds)
		}
	}
	codes := findingCodes(pr)
	for code, severity := range map[string]Severity{"deadline-not-durable": SeverityWarning, "lock-memory-only": SeverityWarning, "buffered-frames-lost": SeverityWarning, "due-commands-bursted": SeverityWarning} {
		if codes[code] != severity {
			t.Errorf("finding %s: want %s got %s", code, severity, codes[code])
		}
	}
	if report.Summary.CommandsDue != 1 || report.Summary.CommandsOverdue != 1 {
		t.Errorf("due/overdue = %d/%d, want 1/1", report.Summary.CommandsDue, report.Summary.CommandsOverdue)
	}
	if report.Summary.MissingComponents == 0 {
		t.Error("memory-only components must be reported")
	}
	if report.Summary.RecordingsActive != 1 || report.Summary.RFChainsTuned != 2 || report.Summary.AntennasOwned != 1 {
		t.Errorf("unexpected summary: %+v", report.Summary)
	}
}

func TestTerminalPassStrandsAntennaAndReservation(t *testing.T) {
	f := newFixture(t)
	passID, resID := "PASS-DONE", "RES-9"
	f.passes = []passsvc.Pass{{
		ID: passID, SatelliteID: "SAT-A12", AntennaID: "ANT-02",
		RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"},
		Window: passsvc.Window{AOS: f.now.Add(-10 * time.Minute), LOS: f.now.Add(-2 * time.Minute), Revision: 1},
		Phase:  passsvc.Completed, RecordingSegmentID: "SEG-9", OwnerStation: "north", OwnerGeneration: 1,
	}}
	f.interval = []scheduler.Interval{{ReservationID: resID, PassID: passID, AntennaID: "ANT-02", Start: f.now.Add(-11 * time.Minute), End: f.now.Add(-2 * time.Minute), Generation: 1, Status: scheduler.Active}}
	f.antennas = []antenna.State{{ID: "ANT-02", Mode: antenna.Tracking, OwnerPassID: passID, OwnerGeneration: 1}}
	f.segments = []recorder.Segment{{ID: "SEG-9", PassID: passID, OwnerStation: "north", OwnerGeneration: 1, Status: recorder.Sealed}}
	f.write(t)

	report := f.run(t)
	pr := report.Passes[0]
	codes := findingCodes(pr)
	if codes["owner-stranded"] != SeverityWarning || codes["reservation-stranded"] != SeverityWarning {
		t.Errorf("expected stranded resource findings, got %v", codes)
	}
	if !actionKinds(pr)[ActionReleaseResources] {
		t.Error("expected a release-stranded-resources action")
	}
	if report.Summary.StrandedOwners != 1 {
		t.Errorf("stranded owners = %d, want 1", report.Summary.StrandedOwners)
	}
}

func TestMissingChainAndSegmentAreCriticalAndBlockRecovery(t *testing.T) {
	f := newFixture(t)
	passID := "PASS-BROKEN"
	f.passes = []passsvc.Pass{{
		ID: passID, SatelliteID: "SAT-A12", AntennaID: "ANT-01",
		RFChains: []string{"RF-01", "RF-99"}, RequiredReceiverChains: []string{"RF-01"},
		Window: passsvc.Window{AOS: f.now.Add(-time.Minute), LOS: f.now.Add(5 * time.Minute), Revision: 1},
		Phase:  passsvc.Tracking, RecordingSegmentID: "SEG-GONE", OwnerStation: "north", OwnerGeneration: 1,
	}}
	f.chains = []rf.State{{ChainID: "RF-01", PassID: passID}}
	f.attempts = []receiver.Attempt{{ID: "ATT-1", PassID: passID, Number: 1, Status: receiver.AttemptLocked}}
	f.write(t)

	pr := f.run(t).Passes[0]
	if pr.Recoverable {
		t.Fatalf("pass with missing chain/segment must not be recoverable, findings=%v", pr.Findings)
	}
	codes := findingCodes(pr)
	if codes["chain-missing"] != SeverityCritical || codes["segment-missing"] != SeverityCritical {
		t.Errorf("expected critical chain-missing/segment-missing, got %v", codes)
	}
}

func TestDrillIsReadOnlyAcrossSnapshots(t *testing.T) {
	f := newFixture(t)
	passID := "PASS-RO"
	f.passes = []passsvc.Pass{{
		ID: passID, SatelliteID: "SAT-A12", AntennaID: "ANT-01", RFChains: []string{"RF-01"},
		RequiredReceiverChains: []string{"RF-01"},
		Window:                 passsvc.Window{AOS: f.now.Add(3 * time.Minute), LOS: f.now.Add(9 * time.Minute), Revision: 1},
		Phase:                  passsvc.Planned, ReservationID: "RES-RO", OwnerStation: "north", OwnerGeneration: 1,
	}}
	f.interval = []scheduler.Interval{{ReservationID: "RES-RO", PassID: passID, AntennaID: "ANT-01", Start: f.now.Add(2 * time.Minute), End: f.now.Add(9 * time.Minute), Generation: 1, Status: scheduler.Reserved}}
	f.commands = []scheduler.PointingCommand{{ID: "CMD-RO", PassID: passID, AntennaID: "ANT-01", Revision: 1, ExecuteAt: f.now.Add(3 * time.Minute), Azimuth: 100, Elevation: 10}}
	f.write(t)

	before := hashTree(t, f.paths.Root)
	report := f.run(t)
	after := hashTree(t, f.paths.Root)
	if fmt.Sprintf("%v", before) != fmt.Sprintf("%v", after) {
		t.Fatalf("drill mutated durable state:\nbefore=%v\nafter =%v", before, after)
	}
	if len(report.Components) != 8 {
		t.Errorf("expected 8 replayed documents, got %d", len(report.Components))
	}
}

func TestDrillOnEmptyStateRoot(t *testing.T) {
	f := newFixture(t)
	report := f.run(t)
	if report.Summary.PassesTotal != 0 || len(report.MissingComponents) == 0 {
		t.Errorf("empty root should replay zero passes but still list missing components: %+v", report.Summary)
	}
}

func hashTree(t *testing.T, root string) []string {
	t.Helper()
	var lines []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		hash := sha256.New()
		if _, err := io.Copy(hash, file); err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		lines = append(lines, fmt.Sprintf("%s %x", rel, hash.Sum(nil)))
		return nil
	})
	if err != nil {
		t.Fatalf("hash tree: %v", err)
	}
	sort.Strings(lines)
	return lines
}
