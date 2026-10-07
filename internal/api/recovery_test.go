package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/recovery"
	"orbitlink/internal/rf"
)

func TestRecoveryDrillEndpointsRehearseWithoutTouchingLiveState(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()

	// Drive the seeded pass into tracking with an open recording, an
	// undrained frame and a transmit-enabled chain.
	value := runtime.Passes.List()[0]
	for _, chainID := range []string{"RF-01", "RF-02"} {
		if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: chainID, CenterHz: 2_245_000_000, DopplerHz: 12_000, Polarization: rf.RightCircular}); err != nil {
			t.Fatal(err)
		}
	}
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 7},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 7},
	}
	if _, err := runtime.Passes.AcceptDiversity(value.ID, results); err != nil {
		t.Fatal(err)
	}
	segment, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found {
		t.Fatal("tracking pass has no recording")
	}
	if err := runtime.Recorder.Buffer(segment.ID, recorder.Frame{Sequence: 1, Payload: []byte("telemetry")}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.RF.EnableTransmit("RF-01"); err != nil {
		t.Fatal(err)
	}

	liveFiles := []string{"passes.json", "pointing-commands.json", "antennas.json", "rf-chains.json", "recording-sessions.json"}
	before := make(map[string]string, len(liveFiles))
	for _, name := range liveFiles {
		data, err := os.ReadFile(filepath.Join(runtime.StateRoot, "snapshots", name))
		if err != nil {
			t.Fatalf("read live snapshot %s: %v", name, err)
		}
		before[name] = string(data)
	}

	response, err := server.Client().Get(server.URL + "/recovery")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /recovery returned %d", response.StatusCode)
	}
	_ = response.Body.Close()

	response = requestJSON(t, server.Client(), http.MethodPost, server.URL+"/api/recovery/drills", "")
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("POST /api/recovery/drills returned %d", response.StatusCode)
	}
	var report recovery.Report
	decodeBody(t, response, &report)

	if !report.LiveStateUnchanged {
		t.Fatal("drill reported live state changes")
	}
	if report.LiveFilesVerified == 0 {
		t.Fatal("drill did not verify any live snapshots")
	}
	if report.Summary.ActionsFailed != 0 {
		t.Fatalf("sandbox recovery actions failed: %#v", report.Domains)
	}
	passesDomain := domainOf(t, report, "passes")
	if !actionSimulated(passesDomain, value.ID, "resume-tracking") {
		t.Fatalf("tracking pass was not resumed in the drill: %#v", passesDomain.Actions)
	}
	rfDomain := domainOf(t, report, "rf-chains")
	if !actionSimulated(rfDomain, "RF-01", "inhibit-transmit") {
		t.Fatalf("transmit-enabled chain was not inhibited in the drill: %#v", rfDomain.Actions)
	}
	critical := 0
	for _, risk := range rfDomain.Risks {
		if risk.Severity == recovery.RiskCritical {
			critical++
		}
	}
	if critical == 0 {
		t.Fatal("transmit across restart must surface as a critical risk")
	}
	gaps := map[string]bool{}
	for _, missing := range report.Missing {
		gaps[missing.Component] = true
	}
	for _, component := range []string{"pass-deadlines", "handover-sessions", "decoder-sessions", "recorder-buffer"} {
		if !gaps[component] {
			t.Fatalf("persistence gap %s missing from report", component)
		}
	}

	// Live queues and device snapshots are byte-identical after the drill.
	for _, name := range liveFiles {
		data, err := os.ReadFile(filepath.Join(runtime.StateRoot, "snapshots", name))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != before[name] {
			t.Fatalf("live snapshot %s changed during the drill", name)
		}
	}
	if current, _ := runtime.Passes.Get(value.ID); current.Phase != passsvc.Tracking {
		t.Fatalf("live pass mutated to %s", current.Phase)
	}
	if state, _ := runtime.RF.State("RF-01"); !state.TransmitEnabled {
		t.Fatal("drill inhibited the live RF chain")
	}
	if _, found := runtime.Recorder.ActiveForPass(value.ID); !found {
		t.Fatal("drill sealed the live recording")
	}

	response, err = server.Client().Get(server.URL + "/api/recovery/drills")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Items []recovery.Report `json:"items"`
	}
	decodeBody(t, response, &list)
	if len(list.Items) != 1 || list.Items[0].ID != report.ID {
		t.Fatalf("drill history did not retain the report: %#v", list.Items)
	}

	response, err = server.Client().Get(server.URL + "/api/recovery/drills/" + report.ID)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET drill by id returned %d", response.StatusCode)
	}
	_ = response.Body.Close()

	response, err = server.Client().Get(server.URL + "/api/recovery/drills/unknown")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown drill returned %d", response.StatusCode)
	}
	_ = response.Body.Close()
}

func domainOf(t *testing.T, report recovery.Report, name string) recovery.DomainReplay {
	t.Helper()
	for _, domain := range report.Domains {
		if domain.Domain == name {
			return domain
		}
	}
	t.Fatalf("domain %s missing from drill report", name)
	return recovery.DomainReplay{}
}

func actionSimulated(domain recovery.DomainReplay, subject, verb string) bool {
	for _, action := range domain.Actions {
		if action.Subject == subject && action.Verb == verb {
			return action.Simulated
		}
	}
	return false
}
