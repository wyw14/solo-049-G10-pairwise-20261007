// Package recovery rehearses a process restart against a read-only copy of
// the station state. A drill replays persisted passes, reservations, antenna
// ownership, RF chains and recording sessions inside a temporary sandbox,
// simulates the actions that would bring the station back, and reports the
// risks and the components that lack durable state. The live state root is
// only read, never written, and the report proves it with before/after
// checksums of every live snapshot.
package recovery

import "time"

// Severity levels for drill risks.
const (
	RiskWarning  = "warning"
	RiskCritical = "critical"
)

// Missing component kinds.
const (
	MissingSnapshotFile   = "snapshot-file"
	MissingPersistenceGap = "persistence-gap"
)

// Report is the outcome of one recovery drill.
type Report struct {
	ID                 string             `json:"id"`
	StartedAt          time.Time          `json:"started_at"`
	FinishedAt         time.Time          `json:"finished_at"`
	StateRoot          string             `json:"state_root"`
	RebootedAt         time.Time          `json:"rebooted_at"`
	Domains            []DomainReplay     `json:"domains"`
	Missing            []MissingComponent `json:"missing"`
	Risks              []Risk             `json:"risks,omitempty"`
	Summary            Summary            `json:"summary"`
	LiveFilesVerified  int                `json:"live_files_verified"`
	LiveStateUnchanged bool               `json:"live_state_unchanged"`
}

// DomainReplay covers one recovered domain inside the read-only copy.
type DomainReplay struct {
	Domain    string         `json:"domain"`
	Component string         `json:"component"`
	Recovered int            `json:"recovered"`
	Entities  []EntityReplay `json:"entities,omitempty"`
	Actions   []Action       `json:"actions,omitempty"`
	Risks     []Risk         `json:"risks,omitempty"`
}

// EntityReplay is one persisted entity as it came back after the restart.
type EntityReplay struct {
	ID     string `json:"id"`
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
}

// Action is a recovery step the station would take after the restart. Every
// action is simulated against the read-only copy; Simulated reports whether
// the step succeeded there.
type Action struct {
	Subject   string `json:"subject"`
	Verb      string `json:"verb"`
	Detail    string `json:"detail"`
	Simulated bool   `json:"simulated"`
	Outcome   string `json:"outcome,omitempty"`
}

// Risk is something that can go wrong or lose data across the restart.
type Risk struct {
	Subject  string `json:"subject"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}

// MissingComponent is durable state the restart could not find: either a
// snapshot file that is absent from the state root, or a runtime component
// that has no persistence at all.
type MissingComponent struct {
	Component string `json:"component"`
	Kind      string `json:"kind"`
	Detail    string `json:"detail"`
}

// Summary rolls the drill up for dashboards.
type Summary struct {
	EntitiesRecovered int `json:"entities_recovered"`
	ActionsTotal      int `json:"actions_total"`
	ActionsSimulated  int `json:"actions_simulated"`
	ActionsFailed     int `json:"actions_failed"`
	RisksCritical     int `json:"risks_critical"`
	RisksWarning      int `json:"risks_warning"`
	MissingTotal      int `json:"missing_total"`
}

func (r *Report) summarize() {
	summary := Summary{MissingTotal: len(r.Missing)}
	countRisk := func(risk Risk) {
		if risk.Severity == RiskCritical {
			summary.RisksCritical++
		} else {
			summary.RisksWarning++
		}
	}
	for _, risk := range r.Risks {
		countRisk(risk)
	}
	for _, domain := range r.Domains {
		summary.EntitiesRecovered += domain.Recovered
		for _, action := range domain.Actions {
			summary.ActionsTotal++
			if action.Simulated {
				summary.ActionsSimulated++
			} else {
				summary.ActionsFailed++
			}
		}
		for _, risk := range domain.Risks {
			countRisk(risk)
		}
	}
	r.Summary = summary
}
