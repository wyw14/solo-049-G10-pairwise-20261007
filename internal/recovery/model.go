package recovery

import "time"

// Severity ranks the operational impact of a replay finding.
type Severity string

const (
	SeverityOK       Severity = "ok"
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// ActionKind enumerates the verbs a restarted process would have to perform.
type ActionKind string

const (
	// ActionRebuildDeadline reconstructs a LOS or monotonic deadline.
	ActionRebuildDeadline ActionKind = "rebuild-deadline"
	// ActionReplayDueCommands flushes due pointing commands back at the antenna.
	ActionReplayDueCommands ActionKind = "replay-due-commands"
	// ActionReacquireLock re-drives carrier acquisition after the in-memory lock vanished.
	ActionReacquireLock ActionKind = "reacquire-lock"
	// ActionReopenDecoder rebuilds the decoder epoch and symbol assembler.
	ActionReopenDecoder ActionKind = "reopen-decoder"
	// ActionResumeDrain continues flushing an open recording segment to disk.
	ActionResumeDrain ActionKind = "resume-drain"
	// ActionResumeFinalize re-drives unlock, drain and seal for a stuck pass.
	ActionResumeFinalize ActionKind = "resume-finalize"
	// ActionRebindReservation recreates an antenna reservation the snapshot no longer holds.
	ActionRebindReservation ActionKind = "rebind-reservation"
	// ActionReassertRFPlan pushes a frequency plan back onto a chain that lost it.
	ActionReassertRFPlan ActionKind = "reassert-rf-plan"
	// ActionReleaseResources frees reservations and antenna owners stranded by a terminal pass.
	ActionReleaseResources ActionKind = "release-stranded-resources"
)

// Action is one recoverable (or blocked) step inferred during the replay.
type Action struct {
	Kind        ActionKind `json:"kind"`
	Description string     `json:"description"`
	// Recoverable reports whether durable state is sufficient to perform the action.
	Recoverable bool `json:"recoverable"`
	// Automatic reports whether the current boot path performs it without an operator.
	Automatic bool `json:"automatic"`
	// Requires names the missing durable component or external trigger, if any.
	Requires string `json:"requires,omitempty"`
}

// Finding describes a risk or inconsistency observed while replaying state.
type Finding struct {
	Code      string   `json:"code"`
	Severity  Severity `json:"severity"`
	Component string   `json:"component"`
	Subject   string   `json:"subject,omitempty"`
	Message   string   `json:"message"`
}

// MissingComponent is state the running process held only in memory, so a fresh
// process replaying the durable snapshots can never reconstruct it.
type MissingComponent struct {
	Component string   `json:"component"`
	Reason    string   `json:"reason"`
	Impact    string   `json:"impact"`
	Affected  []string `json:"affected,omitempty"`
}

// ComponentReplay summarizes one durable document replayed into the "restarted" view.
type ComponentReplay struct {
	Component string    `json:"component"`
	Replayed  int       `json:"replayed"`
	Detail    string    `json:"detail,omitempty"`
	Findings  []Finding `json:"findings,omitempty"`
}

// PassRecovery is the per-pass replay result.
type PassRecovery struct {
	PassID      string `json:"pass_id"`
	SatelliteID string `json:"satellite_id"`
	AntennaID   string `json:"antenna_id"`
	Phase       string `json:"phase"`
	// Recoverable is false when a critical finding cannot be healed from disk.
	Recoverable bool      `json:"recoverable"`
	Actions     []Action  `json:"actions,omitempty"`
	Findings    []Finding `json:"findings,omitempty"`
}

// Summary rolls the replay up for a quick operator glance.
type Summary struct {
	PassesTotal       int `json:"passes_total"`
	PassesAtRisk      int `json:"passes_at_risk"`
	PassesRecoverable int `json:"passes_recoverable"`
	ReservationsLive  int `json:"reservations_live"`
	AntennasOwned     int `json:"antennas_owned"`
	RFChainsTuned     int `json:"rf_chains_tuned"`
	RecordingsActive  int `json:"recordings_active"`
	CommandsDue       int `json:"commands_due"`
	CommandsOverdue   int `json:"commands_overdue"`
	StrandedOwners    int `json:"stranded_owners"`
	MissingComponents int `json:"missing_components"`
}

// Report is the immutable result of one recovery drill. It never carries live
// manager references and is safe to return over the API or render read-only.
type Report struct {
	GeneratedAt        time.Time          `json:"generated_at"`
	SimulatedRestartAt time.Time          `json:"simulated_restart_at"`
	StateRoot          string             `json:"state_root"`
	ReadOnly           bool               `json:"read_only"`
	Summary            Summary            `json:"summary"`
	Components         []ComponentReplay  `json:"components"`
	Passes             []PassRecovery     `json:"passes"`
	MissingComponents  []MissingComponent `json:"missing_components"`
	GlobalFindings     []Finding          `json:"global_findings"`
}
