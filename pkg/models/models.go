package models

import (
	"database/sql"
	"time"
)

// Company represents an Autotask company
type Company struct {
	ID           int64          `db:"id" json:"id"`
	Name         string         `db:"name" json:"name"`
	AddressLine1 sql.NullString `db:"address_line1" json:"addressLine1,omitempty"`
	AddressLine2 sql.NullString `db:"address_line2" json:"addressLine2,omitempty"`
	City         sql.NullString `db:"city" json:"city,omitempty"`
	State        sql.NullString `db:"state" json:"state,omitempty"`
	PostalCode   sql.NullString `db:"postal_code" json:"postalCode,omitempty"`
	Country      sql.NullString `db:"country" json:"country,omitempty"`
	Phone        sql.NullString `db:"phone" json:"phone,omitempty"`
	Active       sql.NullBool   `db:"active" json:"active,omitempty"`
	CreatedAt    sql.NullTime   `db:"created_at" json:"createdAt,omitempty"`
	UpdatedAt    sql.NullTime   `db:"updated_at" json:"updatedAt,omitempty"`
	SyncAt       time.Time      `db:"sync_at" json:"syncAt"`
}

// Contact represents an Autotask contact
type Contact struct {
	ID        int64          `db:"id" json:"id"`
	CompanyID sql.NullInt64  `db:"company_id" json:"companyId,omitempty"`
	FirstName sql.NullString `db:"first_name" json:"firstName,omitempty"`
	LastName  sql.NullString `db:"last_name" json:"lastName,omitempty"`
	Email     sql.NullString `db:"email" json:"email,omitempty"`
	Phone     sql.NullString `db:"phone" json:"phone,omitempty"`
	Mobile    sql.NullString `db:"mobile" json:"mobile,omitempty"`
	Active    sql.NullBool   `db:"active" json:"active,omitempty"`
	CreatedAt sql.NullTime   `db:"created_at" json:"createdAt,omitempty"`
	UpdatedAt sql.NullTime   `db:"updated_at" json:"updatedAt,omitempty"`
	SyncAt    time.Time      `db:"sync_at" json:"syncAt"`
}

// Ticket represents an Autotask ticket
type Ticket struct {
	ID                  int64          `db:"id" json:"id"`
	Title               string         `db:"title" json:"title"`
	Description         sql.NullString `db:"description" json:"description,omitempty"`
	Status              sql.NullInt64  `db:"status" json:"status,omitempty"`
	Priority            sql.NullInt64  `db:"priority" json:"priority,omitempty"`
	QueueID             sql.NullInt64  `db:"queue_id" json:"queueId,omitempty"`
	CompanyID           sql.NullInt64  `db:"company_id" json:"companyId,omitempty"`
	ContactID           sql.NullInt64  `db:"contact_id" json:"contactId,omitempty"`
	AssignedResourceID  sql.NullInt64  `db:"assigned_resource_id" json:"assignedResourceId,omitempty"`
	CreatedByResourceID sql.NullInt64  `db:"created_by_resource_id" json:"createdByResourceId,omitempty"`
	CreatedAt           sql.NullTime   `db:"created_at" json:"createdAt,omitempty"`
	UpdatedAt           sql.NullTime   `db:"updated_at" json:"updatedAt,omitempty"`
	CompletedDate       sql.NullTime   `db:"completed_date" json:"completedDate,omitempty"`
	DueDate             sql.NullTime   `db:"due_date" json:"dueDate,omitempty"`
	SyncAt              time.Time      `db:"sync_at" json:"syncAt"`
}

// TimeEntry represents an Autotask time entry
type TimeEntry struct {
	ID            int64           `db:"id" json:"id"`
	TicketID      sql.NullInt64   `db:"ticket_id" json:"ticketId,omitempty"`
	ResourceID    sql.NullInt64   `db:"resource_id" json:"resourceId,omitempty"`
	DateWorked    sql.NullTime    `db:"date_worked" json:"dateWorked,omitempty"`
	StartTime     sql.NullTime    `db:"start_time" json:"startTime,omitempty"`
	EndTime       sql.NullTime    `db:"end_time" json:"endTime,omitempty"`
	HoursWorked   sql.NullFloat64 `db:"hours_worked" json:"hoursWorked,omitempty"`
	Summary       sql.NullString  `db:"summary" json:"summary,omitempty"`
	InternalNotes sql.NullString  `db:"internal_notes" json:"internalNotes,omitempty"`
	NonBillable   sql.NullBool    `db:"non_billable" json:"nonBillable,omitempty"`
	CreatedAt     sql.NullTime    `db:"created_at" json:"createdAt,omitempty"`
	UpdatedAt     sql.NullTime    `db:"updated_at" json:"updatedAt,omitempty"`
	SyncAt        time.Time       `db:"sync_at" json:"syncAt"`
}

// Resource represents an Autotask resource (user/employee)
type Resource struct {
	ID           int64          `db:"id" json:"id"`
	FirstName    sql.NullString `db:"first_name" json:"firstName,omitempty"`
	LastName     sql.NullString `db:"last_name" json:"lastName,omitempty"`
	Email        sql.NullString `db:"email" json:"email,omitempty"`
	Active       sql.NullBool   `db:"active" json:"active,omitempty"`
	ResourceType sql.NullInt64  `db:"resource_type" json:"resourceType,omitempty"`
	CreatedAt    sql.NullTime   `db:"created_at" json:"createdAt,omitempty"`
	UpdatedAt    sql.NullTime   `db:"updated_at" json:"updatedAt,omitempty"`
	SyncAt       time.Time      `db:"sync_at" json:"syncAt"`
}

// Contract represents an Autotask contract
type Contract struct {
	ID            int64           `db:"id" json:"id"`
	CompanyID     sql.NullInt64   `db:"company_id" json:"companyId,omitempty"`
	Name          string          `db:"name" json:"name"`
	ContractType  sql.NullInt64   `db:"contract_type" json:"contractType,omitempty"`
	Status        sql.NullInt64   `db:"status" json:"status,omitempty"`
	StartDate     sql.NullTime    `db:"start_date" json:"startDate,omitempty"`
	EndDate       sql.NullTime    `db:"end_date" json:"endDate,omitempty"`
	TimeRemaining sql.NullFloat64 `db:"time_remaining" json:"timeRemaining,omitempty"`
	CreatedAt     sql.NullTime    `db:"created_at" json:"createdAt,omitempty"`
	UpdatedAt     sql.NullTime    `db:"updated_at" json:"updatedAt,omitempty"`
	SyncAt        time.Time       `db:"sync_at" json:"syncAt"`
}

// HistoryRecord represents a history record for any entity
type HistoryRecord struct {
	HistoryID int64     `db:"history_id" json:"historyId"`
	EntityID  int64     `db:"entity_id" json:"entityId"`
	FieldName string    `db:"field_name" json:"fieldName"`
	OldValue  string    `db:"old_value" json:"oldValue"`
	NewValue  string    `db:"new_value" json:"newValue"`
	ChangedAt time.Time `db:"changed_at" json:"changedAt"`
}

// TicketMetrics represents aggregated ticket metrics
type TicketMetrics struct {
	Date               time.Time     `db:"date" json:"date"`
	TicketsCreated     int           `db:"tickets_created" json:"ticketsCreated"`
	TicketsCompleted   int           `db:"tickets_completed" json:"ticketsCompleted"`
	AvgResolutionHours float64       `db:"avg_resolution_hours" json:"avgResolutionHours"`
	AssignedResourceID sql.NullInt64 `db:"assigned_resource_id" json:"assignedResourceId,omitempty"`
	CompanyID          sql.NullInt64 `db:"company_id" json:"companyId,omitempty"`
}

// ResourceUtilization represents resource utilization metrics
type ResourceUtilization struct {
	ResourceID       int64           `db:"resource_id" json:"resourceId"`
	FirstName        sql.NullString  `db:"first_name" json:"firstName,omitempty"`
	LastName         sql.NullString  `db:"last_name" json:"lastName,omitempty"`
	Date             sql.NullTime    `db:"date" json:"date,omitempty"`
	TotalHours       sql.NullFloat64 `db:"total_hours" json:"totalHours,omitempty"`
	BillableHours    sql.NullFloat64 `db:"billable_hours" json:"billableHours,omitempty"`
	NonBillableHours sql.NullFloat64 `db:"non_billable_hours" json:"nonBillableHours,omitempty"`
}

// ContractProfitability represents contract profitability metrics
type ContractProfitability struct {
	ContractID       int64           `db:"contract_id" json:"contractId"`
	ContractName     string          `db:"contract_name" json:"contractName"`
	CompanyID        int64           `db:"company_id" json:"companyId"`
	CompanyName      string          `db:"company_name" json:"companyName"`
	TotalHoursWorked sql.NullFloat64 `db:"total_hours_worked" json:"totalHoursWorked,omitempty"`
}
