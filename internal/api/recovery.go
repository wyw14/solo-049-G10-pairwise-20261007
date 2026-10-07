package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// runRecoveryDrill rehearses a process restart against a read-only copy of
// the station state. The drill never writes to the live state root.
func (r *Runtime) runRecoveryDrill(writer http.ResponseWriter, request *http.Request) {
	report, err := r.Recovery.Run(request.Context())
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, report)
}

func (r *Runtime) listRecoveryDrills(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": r.Recovery.List()})
}

func (r *Runtime) getRecoveryDrill(writer http.ResponseWriter, request *http.Request) {
	report, found := r.Recovery.Get(chi.URLParam(request, "drillID"))
	if !found {
		writeJSON(writer, http.StatusNotFound, map[string]any{"error": "recovery drill not found", "status": http.StatusNotFound})
		return
	}
	writeJSON(writer, http.StatusOK, report)
}
