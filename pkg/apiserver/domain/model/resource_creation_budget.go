package model

import "time"

// ResourceCreationBudget persists the shared creation-rate debt independently
// of administrator-edited policy JSON. A policy update or replica restart must
// not refill the bucket and allow another burst.
type ResourceCreationBudget struct {
	ID             string    `json:"-" gorm:"primaryKey;type:varchar(32);column:id"`
	AvailableAt    time.Time `json:"-" gorm:"precision:6;column:available_at;not null"`
	IntervalMicros int64     `json:"-" gorm:"column:interval_micros;not null;default:0"`
	BaseModel
}

// InitializeUnknownInterval upgrades debt written before the interval was
// persisted. Live debt is conservatively treated as a full burst at the
// current rate, while an even later existing deadline is never shortened.
func (b *ResourceCreationBudget) InitializeUnknownInterval(now time.Time, interval time.Duration, burst int) {
	if b == nil || b.IntervalMicros != 0 {
		return
	}
	if b.AvailableAt.After(now) {
		fullDebt := now.Add(time.Duration(burst) * interval)
		if fullDebt.After(b.AvailableAt) {
			b.AvailableAt = fullDebt
		}
	}
	b.IntervalMicros = int64(interval / time.Microsecond)
}

func (b *ResourceCreationBudget) PrimaryKey() string { return b.ID }
func (b *ResourceCreationBudget) TableName() string {
	return tableNamePrefix + "resource_creation_budget"
}
func (b *ResourceCreationBudget) ShortTableName() string        { return "resource_creation_budget" }
func (b *ResourceCreationBudget) Index() map[string]interface{} { return nil }
