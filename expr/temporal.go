package expr

import (
	"time"

	"github.com/apache/arrow/go/v18/arrow"
)

// LitTimestamp returns an Expr wrapping a LiteralNode of type
// Timestamp(nanosecond, tz). The instant t is stored as nanoseconds since the
// Unix epoch (UTC), matching the canonical type inferLiteralType assigns to a
// bare time.Time literal. tz is the IANA timezone string carried on the Arrow
// type; pass "" for a timezone-naive timestamp.
func LitTimestamp(t time.Time, tz string) Expr {
	typ := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: tz}
	return Expr{Node: LiteralNode{
		Value: arrow.Timestamp(t.UTC().UnixNano()),
		Type:  typ,
	}}
}
