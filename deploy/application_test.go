package deploy

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"gopkg.in/yaml.v3"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	return filepath.Dir(filepath.Dir(file))
}

func TestApplicationManualSync(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "deploy", "argocd", "collab-sphere.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["kind"] != "Application" {
		t.Fatalf("kind = %v", doc["kind"])
	}
	spec, _ := doc["spec"].(map[string]any)
	if spec == nil {
		t.Fatal("missing spec")
	}
	source, _ := spec["source"].(map[string]any)
	if source["path"] != "deploy/helm/collab-sphere" {
		t.Fatalf("path = %v", source["path"])
	}
	dest, _ := spec["destination"].(map[string]any)
	if dest["namespace"] != "collab" {
		t.Fatalf("namespace = %v", dest["namespace"])
	}
	if policy, ok := spec["syncPolicy"].(map[string]any); ok {
		if _, auto := policy["automated"]; auto {
			t.Fatal("automated sync is set")
		}
	}
}
