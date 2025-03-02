package oracle

import "time"

var (
	minOracleTimestamp = getMinOracleTimestamp()
)

type (
	// DataConverter defines the API for conversions to/from
	// go types to oracle datatypes
	DataConverter interface {
		ToOracleTimestamp(t time.Time) time.Time
		FromOracleTimestamp(t time.Time) time.Time
	}
	converter struct{}
)

// NewConverter returns a new instance of DataConverter
func NewConverter() DataConverter {
	return &converter{}
}

// ToOracleTimestamp converts time to Oracle timestamp
func (c *converter) ToOracleTimestamp(t time.Time) time.Time {
	if t.IsZero() {
		return minOracleTimestamp
	}
	return t.UTC().Truncate(time.Microsecond)
}

// FromOracleTimestamp converts Oracle timestamp and returns go time
func (c *converter) FromOracleTimestamp(t time.Time) time.Time {
	if t.Equal(minOracleTimestamp) {
		return time.Time{}.UTC()
	}
	return t.UTC()
}

func getMinOracleTimestamp() time.Time {
	t, err := time.Parse(time.RFC3339, "1000-01-01T00:00:00Z")
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return t.UTC()
}
