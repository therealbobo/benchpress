package processing

import (
	"fmt"
)

type ProcessingOp string

const (
	OpMean   ProcessingOp = "mean"
	OpStdDev ProcessingOp = "stddev"
	OpSum    ProcessingOp = "sum"
	OpMin    ProcessingOp = "min"
	OpMax    ProcessingOp = "max"
)

type ProcessorConfig struct {
	Operations []ProcessingOp `yaml:"operations"`
}

type Processor struct {
	config ProcessorConfig
}

func NewProcessor(cfg ProcessorConfig) *Processor {
	return &Processor{config: cfg,}
}

func (s *Processor) Process(data []map[string]any) (map[string]any, error) {
	intermedieryResult := make(map[string]float64)

	// TODO: support multiple operations
	switch s.config.Operations[0] {
	case OpMean:
		for _, run := range data {
			for key := range run {
				val, ok := intermedieryResult[key]
				if !ok {
					intermedieryResult[key] = run[key].(float64)
				} else {
					intermedieryResult[key] = val + run[key].(float64)
				}
			}
		}
		for key := range intermedieryResult {
			intermedieryResult[key] = intermedieryResult[key] / float64(len(data))
		}
	case OpSum:
		for _, run := range data {
			for key := range run {
				val, ok := intermedieryResult[key]
				if !ok {
					intermedieryResult[key] = run[key].(float64)
				} else {
					intermedieryResult[key] = val + run[key].(float64)
				}
			}
		}
	case OpMin:
		for _, run := range data {
			for key := range run {
				val, ok := intermedieryResult[key]
				if !ok {
					intermedieryResult[key] = run[key].(float64)
				} else {
					if val < run[key].(float64) {
						intermedieryResult[key] = val
					} else {
						intermedieryResult[key] = run[key].(float64)
					}
				}
			}
		}
	case OpMax:
		for _, run := range data {
			for key := range run {
				val, ok := intermedieryResult[key]
				if !ok {
					intermedieryResult[key] = run[key].(float64)
				} else {
					if val > run[key].(float64) {
						intermedieryResult[key] = val
					} else {
						intermedieryResult[key] = run[key].(float64)
					}
				}
			}
		}
	default:
		// TODO: support multiple operations
		return nil, fmt.Errorf("%s aggregate operator not implemented", s.config.Operations[0])
	}

	result := make(map[string]any)

	for key := range intermedieryResult {
		v := intermedieryResult[key]
		if v == float64(int64(v)) {
			result[key] = int64(v)
		} else {
			result[key] = v
		}
	}

	return result, nil
}
