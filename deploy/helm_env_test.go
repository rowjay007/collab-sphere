package deploy

import (
	"bytes"
	"io"
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var requiredEnv = map[string][]string{
	"document-service":   {"DATABASE_URL", "AUTH_PUBLIC_KEY", "INTERNAL_TOKEN", "PERMISSION_ADDR", "GC_INTERVAL", "REPLICA_REPORT_TTL", "METRICS_ADDR"},
	"presence-service":   {"REDIS_URL", "INTERNAL_TOKEN"},
	"comment-service":    {"DATABASE_URL", "DOCUMENT_ADDR", "VERSION_ADDR", "AUTH_PUBLIC_KEY", "INTERNAL_TOKEN", "PERMISSION_ADDR"},
	"version-service":    {"DATABASE_URL", "DOCUMENT_ADDR", "AUTH_PUBLIC_KEY", "INTERNAL_TOKEN", "PERMISSION_ADDR", "RESTORE_HORIZON", "METRICS_ADDR"},
	"permission-service": {"DATABASE_URL", "AUTH_PUBLIC_KEY", "SHARE_PRIVATE_KEY", "INTERNAL_TOKEN", "DOCUMENT_ADDR"},
	"export-service":     {"DATABASE_URL", "AUTH_PUBLIC_KEY", "INTERNAL_TOKEN", "DOCUMENT_ADDR", "VERSION_ADDR", "PERMISSION_ADDR", "S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_REGION"},
	"collab-hub":         {"AUTH_PUBLIC_KEY", "INTERNAL_TOKEN", "PERMISSION_ADDR"},
}

func TestHelmTemplateSetsRequiredEnv(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is missing")
	}
	cmd := exec.Command("helm", "template", "collab", "deploy/helm/collab-sphere")
	cmd.Dir = repoRoot(t)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, out)
	}
	envBySuffix := deploymentEnv(t, out)
	for suffix, keys := range requiredEnv {
		names, ok := envBySuffix[suffix]
		if !ok {
			t.Errorf("missing Deployment %s", suffix)
			continue
		}
		for _, key := range keys {
			if !names[key] {
				t.Errorf("%s missing env %s", suffix, key)
			}
		}
	}
}

func deploymentEnv(t *testing.T, rendered []byte) map[string]map[string]bool {
	t.Helper()
	dec := yaml.NewDecoder(bytes.NewReader(rendered))
	found := map[string]map[string]bool{}
	for {
		var obj map[string]any
		err := dec.Decode(&obj)
		if err == io.EOF {
			return found
		}
		if err != nil {
			t.Fatal(err)
		}
		if obj == nil || obj["kind"] != "Deployment" {
			continue
		}
		meta, _ := obj["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		suffix := ""
		for key := range requiredEnv {
			if strings.HasSuffix(name, key) {
				suffix = key
				break
			}
		}
		if suffix == "" {
			continue
		}
		names := map[string]bool{}
		spec, _ := obj["spec"].(map[string]any)
		tmpl, _ := spec["template"].(map[string]any)
		pod, _ := tmpl["spec"].(map[string]any)
		containers, _ := pod["containers"].([]any)
		for _, raw := range containers {
			c, _ := raw.(map[string]any)
			for _, envRaw := range asList(c["env"]) {
				e, _ := envRaw.(map[string]any)
				if n, _ := e["name"].(string); n != "" {
					names[n] = true
				}
			}
		}
		found[suffix] = names
	}
}

func asList(v any) []any {
	if v == nil {
		return nil
	}
	list, _ := v.([]any)
	return list
}
