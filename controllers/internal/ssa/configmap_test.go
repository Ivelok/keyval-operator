package ssa

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestConfigMapTrim(t *testing.T) {
	source := &corev1.ConfigMap{}
	source.Name = "test"
	source.Namespace = "ns"
	source.Labels = map[string]string{"app": "demo", "user": "keep"}
	source.Annotations = map[string]string{"anno": "value"}
	source.Data = map[string]string{"redis.conf": "port 6379", "sentinel.conf": "monitor"}
	source.BinaryData = map[string][]byte{"binary": {1, 2, 3}}

	result := ConfigMap(source)

	if result.APIVersion != "v1" || result.Kind != "ConfigMap" {
		t.Fatalf("unexpected typemeta: %s %s", result.APIVersion, result.Kind)
	}
	if result.Name != source.Name || result.Namespace != source.Namespace {
		t.Fatalf("metadata mismatch: %q %q", result.Name, result.Namespace)
	}
	if len(result.Labels) != len(source.Labels) {
		t.Fatalf("labels not copied: %#v", result.Labels)
	}
	result.Labels["app"] = "other"
	if source.Labels["app"] != "demo" {
		t.Fatalf("labels map should be deep copied")
	}
	if len(result.Data) != len(source.Data) {
		t.Fatalf("data not copied: %#v", result.Data)
	}
	result.Data["redis.conf"] = "modified"
	if source.Data["redis.conf"] == "modified" {
		t.Fatalf("data map should be deep copied")
	}
	if len(result.BinaryData) != 1 {
		t.Fatalf("binary data missing: %#v", result.BinaryData)
	}
	result.BinaryData["binary"][0] = 9
	if source.BinaryData["binary"][0] == 9 {
		t.Fatalf("binary data should be deep copied")
	}
}
