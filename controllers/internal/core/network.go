package core

import (
	"fmt"
	"strconv"
	"strings"

	keyvalv1alpha1 "github.com/ivelok/keyval-operator/api/v1alpha1"
)

func RedisPort(cr *keyvalv1alpha1.KeyValCluster) (int, string) {
	port := 6379
	if cr.Spec.RedisConfig != nil {
		if s, ok := cr.Spec.RedisConfig["port"]; ok && s != "" {
			if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 65535 {
				port = v
			}
		}
	}
	return port, fmt.Sprintf("%d", port)
}

func SentinelPort(cr *keyvalv1alpha1.KeyValCluster) (int, string) {
	if cr.Spec.SentinelService != nil && cr.Spec.SentinelService.Ports != nil && cr.Spec.SentinelService.Ports.Sentinel != 0 {
		p := int(cr.Spec.SentinelService.Ports.Sentinel)
		return p, fmt.Sprintf("%d", p)
	}
	if cr.Spec.SentinelConfig != nil {
		if s, ok := cr.Spec.SentinelConfig["port"]; ok && s != "" {
			if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 65535 {
				return v, fmt.Sprintf("%d", v)
			}
		}
	}
	return 26379, "26379"
}

func PodFQDN(cr *keyvalv1alpha1.KeyValCluster, ordinal int) string {
	return fmt.Sprintf("%s-%d.%s.%s.svc", cr.Name, ordinal, HeadlessName(cr), cr.Namespace)
}

func PodDNSName(cr *keyvalv1alpha1.KeyValCluster, podName string) string {
	if ord := Ordinal(podName); ord >= 0 {
		return PodFQDN(cr, ord)
	}
	return fmt.Sprintf("%s.%s.%s.svc", podName, HeadlessName(cr), cr.Namespace)
}

func Ordinal(name string) int {
	i := strings.LastIndex(name, "-")
	if i < 0 || i+1 >= len(name) {
		return -1
	}
	n := 0
	for _, ch := range name[i+1:] {
		if ch < '0' || ch > '9' {
			return -1
		}
		n = n*10 + int(ch-'0')
	}
	return n
}
