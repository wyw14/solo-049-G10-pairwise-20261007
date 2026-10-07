package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"orbitlink/internal/console"
)

func NewRouter(runtime *Runtime) (http.Handler, error) {
	pages, err := console.NewHandler()
	if err != nil {
		return nil, err
	}
	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Recoverer)
	router.Use(middleware.Timeout(15 * time.Second))
	router.Get("/healthz", console.HealthHandler(runtime))
	router.Handle("/assets/*", console.Static())
	for _, page := range []string{"/passes", "/antennas", "/rf-chains", "/recordings", "/recovery"} {
		router.Get(page, pages.Page(page))
	}
	router.Route("/api", func(api chi.Router) {
		api.Get("/passes", runtime.listPasses)
		api.Post("/passes", runtime.createPass)
		api.Post("/passes/{passID}/reserve", runtime.reservePass)
		api.Post("/passes/{passID}/extend", runtime.extendPass)
		api.Post("/passes/{passID}/acquire", runtime.acquirePass)
		api.Post("/passes/{passID}/diversity", runtime.diversityPass)
		api.Post("/passes/{passID}/result", runtime.deliverAcquisitionResult)
		api.Post("/passes/{passID}/receiver/lock", runtime.lockReceiver)
		api.Post("/passes/{passID}/receiver/loss", runtime.loseCarrier)
		api.Post("/passes/{passID}/receiver/symbols", runtime.feedSymbols)
		api.Post("/passes/{passID}/finalize", runtime.finalizePass)
		api.Post("/passes/{passID}/cancel", runtime.cancelPass)
		api.Post("/passes/{passID}/handover", runtime.handoverPass)
		api.Post("/passes/{passID}/handover/{sessionID}/cleanup", runtime.cleanupHandover)
		api.Post("/ephemeris", runtime.importEphemeris)
		api.Get("/clock/corrections", runtime.listClockCorrections)
		api.Get("/antennas", runtime.listAntennas)
		api.Post("/antennas/execute-due", runtime.executeDuePointing)
		api.Post("/antennas/{antennaID}/stow", runtime.stowAntenna)
		api.Get("/rf-chains", runtime.listRFChains)
		api.Post("/rf-chains/{chainID}/plan", runtime.applyRFPlan)
		api.Post("/rf-chains/{chainID}/transmit", runtime.enableTransmit)
		api.Get("/recordings", runtime.listRecordings)
		api.Post("/recordings/{segmentID}/frames", runtime.appendFrame)
		api.Get("/audit", runtime.listAudit)
		api.Get("/events/{stream}", runtime.listPersistentEvents)
		api.Post("/recovery/drills", runtime.runRecoveryDrill)
	})
	return router, nil
}
