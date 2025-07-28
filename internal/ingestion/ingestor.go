package ingestion

type Source string

const (
	StdoutSrc Source = "stdout"
	StderrSrc Source = "stderr"
)

type IngestorConfig struct {
	JsonIngestorConfig *JsonIngestorConfig `yaml:"json,omitempty" json:"json,omitempty"`
}

type Ingestor interface {
	Select(docs []string) (string, error)
	Standardize(doc string) (map[string]any, error)
	Source() Source
}
