package models

type AnalysisResult struct {
	Subreddit     string
	Title         string
	Body          string
	Categories    []string
	Sentiment     string
	UnixTimestamp int64
}

type DailyTopic struct {
	Topic         string
	Date          string
	PositiveCount int32
	NegativeCount int32
	TotalMentions int32
}
