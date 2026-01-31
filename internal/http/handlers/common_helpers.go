package handlers

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func textPtr(v pgtype.Text) *string {
	if v.Valid {
		return &v.String
	}
	return nil
}

func int4Ptr(v pgtype.Int4) *int32 {
	if v.Valid {
		return &v.Int32
	}
	return nil
}

func int8Ptr(v pgtype.Int8) *int64 {
	if v.Valid {
		return &v.Int64
	}
	return nil
}

func timePtr(v pgtype.Timestamptz) *time.Time {
	if v.Valid {
		return &v.Time
	}
	return nil
}
