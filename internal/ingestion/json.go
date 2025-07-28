package ingestion

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	jmespath "github.com/jmespath-community/go-jmespath"
)

type Aggregate struct {
	Field   string   `yaml:"field" json:"field"`
	GroupBy []string `yaml:"group_by" json:"group_by"`
}

type JsonIngestorConfig struct {
	Source     Source    `yaml:"source,omitempty" json:"source,omitempty"`
	Selector   *string   `yaml:"selector,omitempty" json:"selector,omitempty"`
	Expression *string   `yaml:"expression,omitempty" json:"expression,omitempty"`
	Aggregate  Aggregate `yaml:"aggregate" json:"aggregate"`
}

type jsonIngestor struct {
	config JsonIngestorConfig
}

func NewJsonIngestor(cfg JsonIngestorConfig) *jsonIngestor {
	return &jsonIngestor{config: cfg}
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

func selectFromLine(doc, selector string) (any, error) {
	var data any
	err := json.Unmarshal([]byte(doc), &data)
	if err != nil {
		return nil, err
	}

	return jmespath.Search(selector, data)
}

func (s *jsonIngestor) Select(docs []string) (string, error) {
	var selector string

	if s.config.Expression != nil {
		selector = *s.config.Expression
	} else if s.config.Selector != nil {
		selector = *s.config.Selector
	} else {
		return "", fmt.Errorf("jsonIngestor: no selector provided")
	}

	for _, doc := range docs {
		res, _ := selectFromLine(doc, selector)
		// Ignore the error and try the next docs
		if res != nil && res != false {
			// For the moment just go with the first match
			return doc, nil
		}
	}

	return "", fmt.Errorf("jsonIngestor: no line matched")
}

// Basically there are 3 cases of data:
//  1. [{"key1":1,"key2":100,"value":1337},...] -> groupby
//     -> concat keys
//  2. [1,2,3,4,...] -> simple map
//     -> convert to a map with the index as key
//  3. {"key1": 1337,"key2": 1338,"key3": 1339,...} -> key-value map
//     -> copy over
func (s *jsonIngestor) Standardize(doc string) (map[string]any, error) {
	result := make(map[string]any)

	// no-op filter
	selector := ptr("@")
	if s.config.Selector != nil {
		selector = s.config.Selector
	}

	target, err := selectFromLine(doc, *selector)
	if err != nil {
		return nil, err
	}

	if s.config.Aggregate.GroupBy != nil {
		if !isList(target) {
			return nil, fmt.Errorf(
				"consulting doc \"%s\" using key \"%v\" didn't result in a list"+
					"(which is required with the group_by operator): %v", doc, selector, result)
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
			for _, key := range s.config.Aggregate.GroupBy {
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
			result[key] = sample[s.config.Aggregate.Field]
		}

	} else if isMap(target) {
		result = target.(map[string]any)
	} else {
		if !isList(target) {
			return nil, fmt.Errorf(
				"consulting doc \"%s\" using key \"%v\" didn't result in a list"+
					"(which is required if this is a simple list of values): %v", doc, selector, result)
		}

		for i, e := range target.([]any) {
			key := strconv.Itoa(i)
			result[key] = e
		}
	}

	return result, nil
}

func (s *jsonIngestor) Source() Source {
	return s.config.Source
}
