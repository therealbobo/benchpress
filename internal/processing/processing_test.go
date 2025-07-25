package processing

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

func TestSpec_Standardize(t *testing.T) {
	type fields struct {
		Target    *string
		GroupBy   []string
		Aggregate Aggregate
	}
	type args struct {
		docs []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []map[string]any
		wantErr bool
	}{
		{
			name: "success group by",
			fields: fields{
				Target: ptr(`data."@ns"`),
				GroupBy: []string{
					"min",
					"max",
				},
				Aggregate: Aggregate{
					Field:     "count",
					Operation: "mean",
				},
			},
			args: args{
				docs: []string{
					bpftraceJson,
				},
			},
			want: []map[string]any{
				{
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
			},
			wantErr: false,
		},
		{
			name: "success key-value map",
			fields: fields{
				Target:  nil,
				GroupBy: nil,
				Aggregate: Aggregate{
					Field:     "count",
					Operation: "mean",
				},
			},
			args: args{
				docs: []string{
					`{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": -1, "g": 0}`,
				},
			},
			want: []map[string]any{
				{
					"a": float64(1),
					"b": float64(2),
					"c": float64(3),
					"d": float64(4),
					"e": float64(5),
					"f": float64(-1),
					"g": float64(0),
				},
			},
			wantErr: false,
		},
		{
			name: "success just a list",
			fields: fields{
				Target:  nil,
				GroupBy: nil,
				Aggregate: Aggregate{
					Field:     "count",
					Operation: "mean",
				},
			},
			args: args{
				docs: []string{
					`[1,2,3,4,5,-1,0]`,
				},
			},
			want: []map[string]any{
				{
					"0": float64(1),
					"1": float64(2),
					"2": float64(3),
					"3": float64(4),
					"4": float64(5),
					"5": float64(-1),
					"6": float64(0),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		s := &Spec{
			Target:    tt.fields.Target,
			GroupBy:   tt.fields.GroupBy,
			Aggregate: tt.fields.Aggregate,
		}
		got, err := s.Standardize(tt.args.docs)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Spec.Standardize() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Spec.Standardize() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestSpec_Process(t *testing.T) {
	type fields struct {
		Target    *string
		GroupBy   []string
		Aggregate Aggregate
	}
	type args struct {
		docs []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]any
		wantErr bool
	}{
		{
			name: "success group by aggregate with mean",
			fields: fields{
				Target: nil,
				GroupBy: []string{
					"a",
					"b",
				},
				Aggregate: Aggregate{
					Field:     "c",
					Operation: "mean",
				},
			},
			args: args{
				docs: []string{
					`[{"a":1,"b":2,"c":3},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":-1}]`,
					`[{"a":1,"b":2,"c":8},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":10}]`,
					`[{"a":1,"b":2,"c":8},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":100}]`,
					`[{"a":1,"b":2,"c":7},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":1000}]`,
				},
			},
			want: map[string]any{
				"1-2": float64(6.5),
				"3-4": int64(5),
				"5-6": float64(277.25),
			},
			wantErr: false,
		},
		{
			name: "success group by aggregate with sum",
			fields: fields{
				Target: nil,
				GroupBy: []string{
					"a",
					"b",
				},
				Aggregate: Aggregate{
					Field:     "c",
					Operation: "sum",
				},
			},
			args: args{
				docs: []string{
					`[{"a":1,"b":2,"c":3},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":-1}]`,
					`[{"a":1,"b":2,"c":8},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":10}]`,
					`[{"a":1,"b":2,"c":8},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":100}]`,
					`[{"a":1,"b":2,"c":7},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":1000}]`,
				},
			},
			want: map[string]any{
				"1-2": int64(26),
				"3-4": int64(20),
				"5-6": int64(1109),
			},
			wantErr: false,
		},
		{
			name: "success group by aggregate with sum",
			fields: fields{
				Target: nil,
				GroupBy: []string{
					"a",
					"b",
				},
				Aggregate: Aggregate{
					Field:     "c",
					Operation: "min",
				},
			},
			args: args{
				docs: []string{
					`[{"a":1,"b":2,"c":3},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":-1}]`,
					`[{"a":1,"b":2,"c":8},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":10}]`,
					`[{"a":1,"b":2,"c":8},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":100}]`,
					`[{"a":1,"b":2,"c":7},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":1000}]`,
				},
			},
			want: map[string]any{
				"1-2": int64(3),
				"3-4": int64(5),
				"5-6": int64(-1),
			},
			wantErr: false,
		},
		{
			name: "success group by aggregate with sum",
			fields: fields{
				Target: nil,
				GroupBy: []string{
					"a",
					"b",
				},
				Aggregate: Aggregate{
					Field:     "c",
					Operation: "max",
				},
			},
			args: args{
				docs: []string{
					`[{"a":1,"b":2,"c":3},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":-1}]`,
					`[{"a":1,"b":2,"c":8},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":10}]`,
					`[{"a":1,"b":2,"c":8},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":100}]`,
					`[{"a":1,"b":2,"c":7},{"a":3,"b":4,"c":5},{"a":5,"b":6,"c":1000}]`,
				},
			},
			want: map[string]any{
				"1-2": int64(8),
				"3-4": int64(5),
				"5-6": int64(1000),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		s := &Spec{
			Target:    tt.fields.Target,
			GroupBy:   tt.fields.GroupBy,
			Aggregate: tt.fields.Aggregate,
		}
		got, err := s.Process(tt.args.docs)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Spec.Process() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Spec.Process() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
