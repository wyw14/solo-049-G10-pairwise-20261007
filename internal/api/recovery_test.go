package api

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestRecoveryDrillEndpointIsReadOnly(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()

	before := hashStateTree(t, runtime.StateRoot)
	response, err := server.Client().Post(server.URL+"/api/recovery/drills", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("drill returned %d", response.StatusCode)
	}
	var report struct {
		ReadOnly bool `json:"read_only"`
		Summary  struct {
			PassesTotal       int `json:"passes_total"`
			MissingComponents int `json:"missing_components"`
		} `json:"summary"`
		Components []struct {
			Component string `json:"component"`
			Replayed  int    `json:"replayed"`
		} `json:"components"`
	}
	if err := json.NewDecoder(response.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	if !report.ReadOnly {
		t.Error("drill response must be read_only")
	}
	if report.Summary.PassesTotal == 0 {
		t.Error("seeded runtime should replay at least one pass")
	}
	if report.Summary.MissingComponents == 0 {
		t.Error("drill must list memory-only components")
	}
	if len(report.Components) != 8 {
		t.Errorf("expected 8 replayed snapshots, got %d", len(report.Components))
	}
	after := hashStateTree(t, runtime.StateRoot)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("recovery drill mutated durable state:\nbefore=%v\nafter=%v", before, after)
	}

	// The live managers must still serve their normal views untouched.
	if got := len(runtime.Passes.List()); got != report.Summary.PassesTotal {
		t.Errorf("live pass count changed after drill: %d", got)
	}
}

func hashStateTree(t *testing.T, root string) []string {
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
		t.Fatal(err)
	}
	sort.Strings(lines)
	return lines
}
