package controllers

import (
	"time"

	opreplication "github.com/ivelok/keyval-operator/controllers/internal/ops/replication"
)

// MasterAddressCache aliases the internal master-address cache type so callers outside the controllers
// package can reference it without importing internal packages.
type MasterAddressCache = opreplication.MasterAddressCache

// NewMasterAddressCache constructs the internal master-address cache for reconcile tuning.
func NewMasterAddressCache(ttl time.Duration, capacity int) *MasterAddressCache {
	return opreplication.NewMasterAddressCache(ttl, capacity)
}
