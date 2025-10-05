package controllers

import (
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

func TestShouldEmitNoGoodSlaveEventConcurrent(t *testing.T) {
	t.Parallel()
	cr := &keyvalv1alpha1.KeyValCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}
	goodSlaveEventCooldown.mu.Lock()
	goodSlaveEventCooldown.last = map[string]time.Time{}
	goodSlaveEventCooldown.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = shouldEmitNoGoodSlaveEvent(cr)
		}()
	}
	wg.Wait()

	if shouldEmitNoGoodSlaveEvent(cr) {
		t.Fatalf("cooldown should suppress immediate re-emission")
	}
}
