package ingestion

type Ingestor interface {
	Select(docs []string) (string, error)
	Standardize(doc string) (map[string]any, error)
}
