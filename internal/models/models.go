package models

type AnalysisResult struct {
	Subreddit     string
	Title         string
	Body          string
	Categories    []string
	Sentiment     string
	UnixTimestamp int64
}
