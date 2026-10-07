package api

import (
	"net/http"
	"time"

	"orbitlink/internal/recovery"
)

// runRecoveryDrill replays the durable snapshots without touching the live
// command queue or device managers. The drill reads files through r.Paths only.
func (r *Runtime) runRecoveryDrill(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		// SimulatedRestartAt optionally pins the "boot" instant; defaults to now.
		SimulatedRestartAt *time.Time `json:"simulated_restart_at"`
	}
	if request.ContentLength > 0 {
		if err := decodeJSON(request, &input); err != nil {
			writeError(writer, err)
			return
		}
	}
	now := r.Clock.Now()
	if input.SimulatedRestartAt != nil {
		now = input.SimulatedRestartAt.UTC()
	}
	report, err := recovery.NewDrill(r.Paths).Run(now)
	if err != nil {
		writeError(writer, err)
		return
	}
	// Defensive: a replay must never present itself as able to mutate state.
	report.ReadOnly = true
	writeJSON(writer, http.StatusOK, report)
}
