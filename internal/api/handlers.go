package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/zeidlitz/data-publisher/internal/models"
)

func (a *Application) GetAnalysisResults(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	res, err := a.dbClient.GetAnalysisResults(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var payload []models.AnalysisResult
	for _, r := range res {
		payload = append(payload, r)
	}

	responseBody, _ := json.Marshal(payload)
	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}

func (a *Application) GetDailyTopics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Extract segments from the URL
	year, _ := strconv.Atoi(r.PathValue("year"))
	month, _ := strconv.Atoi(r.PathValue("month"))
	day, _ := strconv.Atoi(r.PathValue("day"))

	res, err := a.dbClient.GetDailyTopics(ctx, year, month, day)
	if err != nil {
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, "Encoding error", http.StatusInternalServerError)
	}
}

func (a *Application) Ping(w http.ResponseWriter, r *http.Request) {
	type Payload struct {
		Message string `json:"message"`
	}

	payload := Payload{
		Message: "Pong",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}
