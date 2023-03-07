package continuoustest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnmarshalHistogramTypes(t *testing.T) {
	testCases := []struct {
		in          string
		expected    HistogramTypes
		expectedErr string
	}{
		{
			in: "counter,int",
			expected: HistogramTypes{
				{
					MetricType: Counter,
					DataType:   IntHistogram,
				},
			},
		},
		{
			in: "counter,int;counter,float;gauge,int;gauge,float",
			expected: HistogramTypes{
				{
					MetricType: Counter,
					DataType:   IntHistogram,
				},
				{
					MetricType: Counter,
					DataType:   FloatHistogram,
				},
				{
					MetricType: Gauge,
					DataType:   IntHistogram,
				},
				{
					MetricType: Gauge,
					DataType:   FloatHistogram,
				},
			},
		},
		{
			in:          "foobar,int",
			expectedErr: "unrecognized metric type: foobar",
		},
		{
			in:          "counter,foobaz",
			expectedErr: "unrecognized data type: foobaz",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			var hts HistogramTypes
			err := hts.UnmarshalText([]byte(tc.in))

			if tc.expectedErr != "" {
				require.Equal(t, tc.expectedErr, err.Error())
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, hts)
		})
	}
}
