package session

import (
	go_ora "github.com/sijms/go-ora/v2"
	"time"
)

// TimeStamp is wrapper for driver's TimeStamp.
// the idea is to put as more as possible driver related stuff out of main plugin package
// in case of replacing the driver only this piece has to be modified in terms of time handling
// there is not way in current design if sqlplugin package to hide it from higher packages, so leaving it here
type TimeStamp struct {
	go_ora.TimeStamp
}

func (t TimeStamp) ToTime() time.Time {
	return time.Time(t.TimeStamp)
}

func (t TimeStamp) AsParam() go_ora.TimeStamp {
	return t.TimeStamp
}

func NewTimeStamp(origTime time.Time) TimeStamp {
	return TimeStamp{go_ora.TimeStamp(origTime)}
}

func GetOraTimeStampPtr(origTime *time.Time) *go_ora.TimeStamp {
	if origTime == nil {
		return nil
	}

	v := go_ora.TimeStamp(*origTime)

	return &v
}

func GetTimeStampPtrFromOra(oraTime *go_ora.TimeStamp) *time.Time {
	if oraTime == nil {
		return nil
	}

	v := time.Time(*oraTime)

	return &v
}
