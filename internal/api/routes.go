package api

import (
	"net/http"
)

func (a *Application) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", a.Ping)
	mux.HandleFunc("GET /analysis", a.GetAnalysisResults)
	mux.HandleFunc("GET /daily_topics/{year}/{month}/{day}", a.GetDailyTopics)
	return mux
}
