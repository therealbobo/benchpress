package processing

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	jmespath "github.com/jmespath-community/go-jmespath"
)

type AggregateOp string

const (
	OpMean   AggregateOp = "mean"
	OpStdDev AggregateOp = "stddev"
	OpSum    AggregateOp = "sum"
	OpMin    AggregateOp = "min"
	OpMax    AggregateOp = "max"
)

type Aggregate struct {
	Field     string      `yaml:"field" json:"field"`
	Operation AggregateOp `yaml:"operation" json:"operation"`
}

type Spec struct {
	Target    *string   `yaml:"target,omitempty" json:"target,omitempty"`
	GroupBy   []string  `yaml:"group_by, omitempty" json:"group_by,omitempty"`
	Aggregate Aggregate `yaml:"aggregate" json:"aggregate"`
}

func ptr(s string) *string {
	return &s
}

func isList(i any) bool {
	kind := reflect.TypeOf(i).Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

func isMap(i any) bool {
	kind := reflect.TypeOf(i).Kind()
	return kind == reflect.Map
}

// Basically there are 3 cases of data:
//  1. [{"key1":1,"key2":100,"value":1337},...] -> groupby
//     -> concat keys
//  2. [1,2,3,4,...] -> simple map
//     -> convert to a map with the index as key
//  3. {"key1": 1337,"key2": 1338,"key3": 1339,...} -> key-value map
//     -> copy over
func (s *Spec) Standardize(docs []string) ([]map[string]any, error) {
	var data any
	var result []map[string]any

	for _, doc := range docs {
		partial := make(map[string]any)
		err := json.Unmarshal([]byte(doc), &data)
		if err != nil {
			return nil, err
		}

		if s.Target == nil {
			// no-op filter
			s.Target = ptr("@")
		}
		target, err := jmespath.Search(*s.Target, data)
		if err != nil {
			return nil, err
		}

		if s.GroupBy != nil {
			if !isList(target) {
				return nil, fmt.Errorf(
					"consulting doc \"%s\" using key \"%v\" didn't result in a list"+
						"(which is required with the group_by operator): %v", doc, s.Target, result)
			}

			// This whole is a "sample":
			// {
			//   "min": 256,
			//   "max": 511,
			//   "count": 4
			// },
			for _, e := range target.([]any) {
				var groupedKeys []string
				sample := e.(map[string]any)
				for _, key := range s.GroupBy {
					// TODO: Does this code panic if no key if found?
					// TODO: Force whatever key to a string. Do I need
					// to consider other types?

					// keyToGroup -> min, max
					var keyToGroup string
					switch v := sample[key].(type) {
					case float64:
						if v == float64(int64(v)) {
							// Try to convert to integer first
							// (the json decoder decodes numbers as float 64 by default)
							keyToGroup = fmt.Sprintf("%d", int64(v))
						} else {
							keyToGroup = fmt.Sprintf("%f", v)
						}
					case string:
						keyToGroup = v
					default:
						// TODO: what about maps and lists? This should not happen normally
						// or at least I don't see the use case.
						return nil, fmt.Errorf("unexpected type (%v) of key %v", v, sample[key])
					}
					groupedKeys = append(groupedKeys, keyToGroup)
				}
				key := strings.Join(groupedKeys, "-")
				// TODO: what if the key not exists?
				partial[key] = sample[s.Aggregate.Field]
			}

		} else if isMap(target) {
			partial = target.(map[string]any)
		} else {
			if !isList(target) {
				return nil, fmt.Errorf(
					"consulting doc \"%s\" using key \"%v\" didn't result in a list"+
						"(which is required if this is a simple list of values): %v", doc, s.Target, result)
			}

			for i, e := range target.([]any) {
				key := strconv.Itoa(i)
				partial[key] = e
			}
		}

		result = append(result, partial)
	}

	return result, nil
}

func (s *Spec) Process(docs []string) (map[string]any, error) {

	data, err := s.Standardize(docs)
	if err != nil {
		return nil, err
	}

	intermedieryResult := make(map[string]float64)

	// aggregate data here using the Spec.Aggregate.Operation
	switch s.Aggregate.Operation {
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
		return nil, fmt.Errorf("%s aggregate operator not implemented", s.Aggregate.Operation)
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
