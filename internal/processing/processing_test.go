package processing

import (
	"reflect"
	"testing"
)

var testData = []map[string]any{
					{"1-2": float64(3), "3-4": float64(5), "5-6": float64(-1),},
					{"1-2": float64(8), "3-4": float64(5), "5-6": float64(10),},
					{"1-2": float64(8), "3-4": float64(5), "5-6": float64(100),},
					{"1-2": float64(7), "3-4": float64(5), "5-6": float64(1000),},
				}

func TestProcessor_Process(t *testing.T) {
	type fields struct {
		Operation ProcessingOp
	}
	type args struct {
		data []map[string]any
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
				Operation: OpMean,
			},
			args: args{
				data: testData,
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
				Operation: OpSum,
			},
			args: args{
				data: testData,
			},
			want: map[string]any{
				"1-2": int64(26),
				"3-4": int64(20),
				"5-6": int64(1109),
			},
			wantErr: false,
		},
		{
			name: "success group by aggregate with min",
			fields: fields{
				Operation: OpMin,
			},
			args: args{
				data: testData,
			},
			want: map[string]any{
				"1-2": int64(3),
				"3-4": int64(5),
				"5-6": int64(-1),
			},
			wantErr: false,
		},
		{
			name: "success group by aggregate with max",
			fields: fields{
					Operation: OpMax,
			},
			args: args{
				data: testData,
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
		s := &Processor{
			Operation: tt.fields.Operation,
		}
		got, err := s.Process(tt.args.data)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Processor.Process() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Processor.Process() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
