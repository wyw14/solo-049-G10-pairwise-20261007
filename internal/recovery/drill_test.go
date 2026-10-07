package recovery_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/recovery"
	"orbitlink/internal/rf"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

func TestDrillOnEmptyStateRootReportsMissingComponents(t *testing.T) {
	runner := recovery.NewRunner(t.TempDir())
	report, err := runner.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Domains) != 5 {
		t.Fatalf("expected 5 replayed domains, got %d", len(report.Domains))
	}
	if report.Summary.EntitiesRecovered != 0 {
		t.Fatalf("empty root recovered %d entities", report.Summary.EntitiesRecovered)
	}
	snapshotGaps := 0
	persistenceGaps := 0
	for _, missing := range report.Missing {
		switch missing.Kind {
		case recovery.MissingSnapshotFile:
			snapshotGaps++
		case recovery.MissingPersistenceGap:
			persistenceGaps++
		}
	}
	if snapshotGaps != 11 {
		t.Fatalf("expected 11 missing snapshot files, got %d", snapshotGaps)
	}
	if persistenceGaps == 0 {
		t.Fatal("expected memory-only components to be listed as persistence gaps")
	}
	if !report.LiveStateUnchanged || report.LiveFilesVerified != 0 {
		t.Fatalf("empty root live verification: unchanged=%v verified=%d", report.LiveStateUnchanged, report.LiveFilesVerified)
	}
}

func TestDrillRehearsesRecoveryInSandboxOnly(t *testing.T) {
	root := t.TempDir()
	paths := store.NewPaths(root)
	trail := audit.NewTrail(store.NewAppendLog(paths))
	windows, err := store.NewWindowStore(paths)
	if err != nil {
		t.Fatal(err)
	}
	timeline, err := scheduler.NewTimeline(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := antenna.NewFleet(paths, timeline, trail, []string{"ANT-01"})
	if err != nil {
		t.Fatal(err)
	}
	rfController, err := rf.NewController(paths, trail, []string{"RF-01"})
	if err != nil {
		t.Fatal(err)
	}
	recorderManager, err := recorder.NewManager(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	passes, err := passsvc.NewService(paths, recorderManager, trail)
	if err != nil {
		t.Fatal(err)
	}
	recompute := passsvc.NewWindowRecomputer(windows, trail)

	// A planned pass whose window expired while the station was down.
	expired, err := passes.Create(passsvc.CreateRequest{
		SatelliteID: "SAT-A12", AntennaID: "ANT-01", RFChains: []string{"RF-01"},
		RequiredReceiverChains: []string{"RF-01"},
		AOS:                    time.Now().Add(-30 * time.Minute), LOS: time.Now().Add(-20 * time.Minute),
		Revision: 1, OwnerStation: "north",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := recompute.Apply(expired.ID, expired.Window); err != nil {
		t.Fatal(err)
	}

	// A failed pass that still holds a reservation, the antenna and an
	// aborted segment with undrained frames.
	failed, err := passes.Create(passsvc.CreateRequest{
		SatelliteID: "SAT-A12", AntennaID: "ANT-01", RFChains: []string{"RF-01"},
		RequiredReceiverChains: []string{"RF-01"},
		AOS:                    time.Now().Add(-2 * time.Hour), LOS: time.Now().Add(-90 * time.Minute),
		Revision: 1, OwnerStation: "north",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := recompute.Apply(failed.ID, failed.Window); err != nil {
		t.Fatal(err)
	}
	reservation, err := scheduler.NewReservationPlanner(windows, timeline, 30*time.Second).ReservePass(failed.ID, failed.AntennaID)
	if err != nil {
		t.Fatal(err)
	}
	if err := passes.SetReservation(failed.ID, reservation.ReservationID, reservation.Generation); err != nil {
		t.Fatal(err)
	}
	if _, err := fleet.Claim(reservation); err != nil {
		t.Fatal(err)
	}
	if err := passes.BeginAcquisition(failed.ID, "attempt-1"); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: failed.ID, AttemptID: "attempt-1", ChainID: "RF-01", Locked: true, Synced: true, Epoch: 3},
	}
	if _, err := passes.AcceptDiversity(failed.ID, results); err != nil {
		t.Fatal(err)
	}
	segment, found := recorderManager.ActiveForPass(failed.ID)
	if !found {
		t.Fatal("tracking pass has no recorder")
	}
	for sequence := uint64(0); sequence < 2; sequence++ {
		if err := recorderManager.Buffer(segment.ID, recorder.Frame{Sequence: sequence, Payload: []byte("frame")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := passes.ApplyAcquisitionResult(receiver.AcquisitionResult{PassID: failed.ID, AttemptID: "attempt-1", ErrorCode: "CARRIER-LOST"}); err != nil {
		t.Fatal(err)
	}
	if err := rfController.EnableTransmit("RF-01"); err != nil {
		t.Fatal(err)
	}

	snapshotBefore := readSnapshots(t, root)
	runner := recovery.NewRunner(root)
	report, err := runner.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !report.LiveStateUnchanged {
		t.Fatal("drill rewrote live snapshots")
	}
	for name, before := range snapshotBefore {
		after, err := os.ReadFile(filepath.Join(root, "snapshots", name))
		if err != nil {
			t.Fatalf("live snapshot %s vanished: %v", name, err)
		}
		if string(after) != before {
			t.Fatalf("live snapshot %s changed during the drill", name)
		}
	}

	passesDomain := findDomain(t, report, "passes")
	if !hasAction(passesDomain, expired.ID, "mark-failed") {
		t.Fatalf("expired planned pass was not failed in the drill: %#v", passesDomain.Actions)
	}
	reservationsDomain := findDomain(t, report, "reservations")
	if !hasAction(reservationsDomain, reservation.ReservationID, "release-reservation") {
		t.Fatalf("terminal pass reservation was not released: %#v", reservationsDomain.Actions)
	}
	antennasDomain := findDomain(t, report, "antennas")
	if !hasAction(antennasDomain, "ANT-01", "release-ownership") {
		t.Fatalf("stale antenna ownership was not released: %#v", antennasDomain.Actions)
	}
	rfDomain := findDomain(t, report, "rf-chains")
	if !hasAction(rfDomain, "RF-01", "inhibit-transmit") {
		t.Fatalf("transmit-enabled chain was not inhibited: %#v", rfDomain.Actions)
	}
	if !hasRisk(rfDomain, recovery.RiskCritical) {
		t.Fatal("transmit across restart must be a critical risk")
	}
	recordingsDomain := findDomain(t, report, "recordings")
	if !hasRisk(recordingsDomain, recovery.RiskWarning) {
		t.Fatal("lost buffered frames must be reported as a risk")
	}

	// The live components keep their pre-drill state: the failed pass stays
	// failed-but-owned, the chain stays transmit-enabled, the expired pass
	// stays planned.
	if value, _ := passes.Get(expired.ID); value.Phase != passsvc.Planned {
		t.Fatalf("live expired pass mutated to %s", value.Phase)
	}
	if state, _ := fleet.State("ANT-01"); state.OwnerPassID != failed.ID {
		t.Fatalf("live antenna ownership mutated: %#v", state)
	}
	if state, _ := rfController.State("RF-01"); !state.TransmitEnabled {
		t.Fatal("live RF chain was inhibited by the drill")
	}
	if report.Summary.ActionsFailed != 0 {
		t.Fatalf("sandbox simulations failed: %#v", report.Domains)
	}
}

func TestDrillHistoryIsNewestFirst(t *testing.T) {
	runner := recovery.NewRunner(t.TempDir())
	first, err := runner.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := runner.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reports := runner.List()
	if len(reports) != 2 || reports[0].ID != second.ID || reports[1].ID != first.ID {
		t.Fatalf("drill history is not newest first: %#v", reports)
	}
	if _, found := runner.Get(first.ID); !found {
		t.Fatal("stored drill not found by id")
	}
	if _, found := runner.Get("missing"); found {
		t.Fatal("unknown drill id resolved")
	}
}

func findDomain(t *testing.T, report recovery.Report, name string) recovery.DomainReplay {
	t.Helper()
	for _, domain := range report.Domains {
		if domain.Domain == name {
			return domain
		}
	}
	t.Fatalf("domain %s missing from report", name)
	return recovery.DomainReplay{}
}

func hasAction(domain recovery.DomainReplay, subject, verb string) bool {
	for _, action := range domain.Actions {
		if action.Subject == subject && action.Verb == verb {
			return action.Simulated
		}
	}
	return false
}

func hasRisk(domain recovery.DomainReplay, severity string) bool {
	for _, risk := range domain.Risks {
		if risk.Severity == severity {
			return true
		}
	}
	return false
}

func readSnapshots(t *testing.T, root string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "snapshots"))
	if err != nil {
		t.Fatal(err)
	}
	contents := make(map[string]string)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(root, "snapshots", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		contents[entry.Name()] = string(data)
	}
	return contents
}
