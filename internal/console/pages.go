package console

import (
	"fmt"
	"html/template"
	"net/http"
	"path"
)

type Page struct {
	Path        string
	Title       string
	Subtitle    string
	View        string
	Endpoint    string
	ActionLabel string
}

type Handler struct {
	template *template.Template
	pages    map[string]Page
}

func NewHandler() (*Handler, error) {
	source, err := Asset("layout.html")
	if err != nil {
		return nil, err
	}
	tmpl, err := template.New("layout").Parse(string(source))
	if err != nil {
		return nil, fmt.Errorf("parse console template: %w", err)
	}
	pages := []Page{
		{Path: "/passes", Title: "Pass operations", Subtitle: "AOS, LOS and live phase control", View: "passes", Endpoint: "/api/passes", ActionLabel: "Schedule pass"},
		{Path: "/antennas", Title: "Antenna field", Subtitle: "Ownership, pointing and wind-safe motion", View: "antennas", Endpoint: "/api/antennas", ActionLabel: "Refresh field"},
		{Path: "/rf-chains", Title: "RF and acquisition", Subtitle: "Frequency plans and synchronized receiver attempts", View: "rf", Endpoint: "/api/rf-chains", ActionLabel: "Apply receive plan"},
		{Path: "/recordings", Title: "Recording integrity", Subtitle: "Buffered frames, tail drain and sealed segments", View: "recordings", Endpoint: "/api/recordings", ActionLabel: "Refresh segments"},
		{Path: "/recovery", Title: "Task recovery drill", Subtitle: "Read-only replay of passes, reservations, antennas, RF chains and recordings after a restart", View: "recovery", Endpoint: "/api/recovery/drills", ActionLabel: "Run recovery drill"},
	}
	byPath := make(map[string]Page, len(pages))
	for _, page := range pages {
		byPath[page.Path] = page
	}
	return &Handler{template: tmpl, pages: byPath}, nil
}

func (h *Handler) Page(name string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		page, found := h.pages[path.Clean(name)]
		if !found {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		writer.Header().Set("Cache-Control", "no-store")
		if err := h.template.Execute(writer, page); err != nil {
			http.Error(writer, "render console", http.StatusInternalServerError)
		}
	}
}

func Static() http.Handler {
	return http.StripPrefix("/assets/", http.FileServer(http.FS(assets)))
}
