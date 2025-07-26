package ingestion

import (
	"reflect"
	"testing"
)

const bpftraceJson string = `
{
  "type": "hist",
  "data": {
    "@ns": [
      {
        "min": 256,
        "max": 511,
        "count": 4
      },
      {
        "min": 512,
        "max": 1023,
        "count": 10
      },
      {
        "min": 1024,
        "max": 2047,
        "count": 8
      },
      {
        "min": 2048,
        "max": 4095,
        "count": 2
      },
      {
        "min": 4096,
        "max": 8191,
        "count": 24
      },
      {
        "min": 8192,
        "max": 16383,
        "count": 52
      },
      {
        "min": 16384,
        "max": 32767,
        "count": 550
      },
      {
        "min": 32768,
        "max": 65535,
        "count": 147
      },
      {
        "min": 65536,
        "max": 131071,
        "count": 10
      },
      {
        "min": 131072,
        "max": 262143,
        "count": 6
      },
      {
        "min": 262144,
        "max": 524287,
        "count": 1
      },
      {
        "min": 524288,
        "max": 1048575,
        "count": 1
      }
    ]
  }
}
`

func Test_jsonIngestor_Select(t *testing.T) {
	type fields struct {
		Selector   *string
		Expression *string
		Aggregate  Aggregate
	}
	type args struct {
		docs []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "success",
			fields: fields{
				Expression: ptr(`type == 'hist' && contains(keys(data), '@ns')`),
				Selector: ptr(`data."@ns"`),
				Aggregate: Aggregate{
					GroupBy: []string{
						"min",
						"max",
					},
					Field:     "count",
				},
			},
			args: args{
				docs: []string{
					``,
					`{}`,
					`{"type":"mytype"}`,
					`{"type":"hist", "data": {}}`,
					`{"type":"hist", "data": []}`,
					`{"type":"hist", "data": ["@ns"]}`,
					bpftraceJson,
					`{}`,
				},
			},
			want: bpftraceJson,
			wantErr: false,
		},
		{
			name: "success no expression",
			fields: fields{
				Expression: nil,
				Selector: ptr(`data."@ns"`),
				Aggregate: Aggregate{
					GroupBy: []string{
						"min",
						"max",
					},
					Field:     "count",
				},
			},
			args: args{
				docs: []string{
					``,
					`{}`,
					`{"type":"mytype"}`,
					`{"type":"hist", "data": {}}`,
					`{"type":"hist", "data": []}`,
					`{"type":"hist", "data": ["@ns"]}`,
					bpftraceJson,
					`{}`,
				},
			},
			want: bpftraceJson,
			wantErr: false,
		},
		{
			name: "no match",
			fields: fields{
				Expression: ptr(`type == 'hist' && contains(keys(data), '@ns')`),
				Selector: ptr(`data."@ns"`),
				Aggregate: Aggregate{
					GroupBy: []string{
						"min",
						"max",
					},
					Field:     "count",
				},
			},
			args: args{
				docs: []string{
					``,
					`{}`,
					`{"type":"mytype"}`,
					`{"type":"hist", "data": {}}`,
					`{"type":"hist", "data": []}`,
					`{"type":"hist", "data": ["@ns"]}`,
					`{}`,
				},
			},
			want: "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s := &jsonIngestor{
			Selector:   tt.fields.Selector,
			Expression: tt.fields.Expression,
			Aggregate:  tt.fields.Aggregate,
		}
		got, err := s.Select(tt.args.docs)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. jsonIngestor.Select() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("%q. jsonIngestor.Select() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func Test_jsonIngestor_Standardize(t *testing.T) {
	type fields struct {
		Selector   *string
		Expression *string
		Aggregate  Aggregate
	}
	type args struct {
		doc string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]any
		wantErr bool
	}{
		{
			name: "success group by",
			fields: fields{
				Selector: ptr(`data."@ns"`),
				Aggregate: Aggregate{
					GroupBy: []string{
						"min",
						"max",
					},
					Field:     "count",
				},
			},
			args: args{
				doc: bpftraceJson,
			},
			want: map[string]any{
				"256-511":        float64(4),
				"512-1023":       float64(10),
				"1024-2047":      float64(8),
				"2048-4095":      float64(2),
				"4096-8191":      float64(24),
				"8192-16383":     float64(52),
				"16384-32767":    float64(550),
				"32768-65535":    float64(147),
				"65536-131071":   float64(10),
				"131072-262143":  float64(6),
				"262144-524287":  float64(1),
				"524288-1048575": float64(1),
			},
			wantErr: false,
		},
		{
			name: "success key-value map",
			fields: fields{
				Selector:  nil,
				Aggregate: Aggregate{
					GroupBy: nil,
					Field:     "count",
				},
			},
			args: args{
				doc: `{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": -1, "g": 0}`,
			},
			want: map[string]any{
				"a": float64(1),
				"b": float64(2),
				"c": float64(3),
				"d": float64(4),
				"e": float64(5),
				"f": float64(-1),
				"g": float64(0),
			},
			wantErr: false,
		},
		{
			name: "success just a list",
			fields: fields{
				Selector:  nil,
				Aggregate: Aggregate{
					GroupBy: nil,
					Field:     "count",
				},
			},
			args: args{
				doc: `[1,2,3,4,5,-1,0]`,
			},
			want: map[string]any{
				"0": float64(1),
				"1": float64(2),
				"2": float64(3),
				"3": float64(4),
				"4": float64(5),
				"5": float64(-1),
				"6": float64(0),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		s := &jsonIngestor{
			Selector:   tt.fields.Selector,
			Expression: tt.fields.Expression,
			Aggregate:  tt.fields.Aggregate,
		}
		got, err := s.Standardize(tt.args.doc)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. jsonIngestor.Standardize() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. jsonIngestor.Standardize() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
