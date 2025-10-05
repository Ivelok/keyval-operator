package controllers

import (
	"time"

	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
)

// NewMasterAddressCache constructs the internal master-address cache for reconcile tuning.
func NewMasterAddressCache(ttl time.Duration, capacity int) *opreplication.MasterAddressCache {
	return opreplication.NewMasterAddressCache(ttl, capacity)
}
