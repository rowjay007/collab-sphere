package grafana

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"unicode"

	"gopkg.in/yaml.v3"
)

var metricNames = map[string]struct{}{
	"collab_hub_apply_seconds":            {},
	"collab_hub_append_seconds":           {},
	"collab_hub_deliver_seconds":          {},
	"collab_hub_fanout_lag_seconds":       {},
	"collab_hub_connections":              {},
	"collab_hub_policy_dump_seconds":      {},
	"collab_hub_policy_stale_documents":   {},
	"collab_doc_append_seconds":           {},
	"collab_doc_gc_pass_seconds":          {},
	"collab_doc_gc_nodes_removed_total":   {},
	"collab_doc_gc_pinned_replicas":       {},
	"collab_doc_gc_last_success_unixtime": {},
	"collab_doc_gc_stable_age_seconds":    {},
	"collab_export_jobs":                  {},
	"collab_export_job_seconds":           {},
	"collab_grpc_server_seconds":          {},
}

var promQLWords = map[string]struct{}{
	"sum": {}, "min": {}, "max": {}, "avg": {}, "group": {}, "count": {},
	"stddev": {}, "stdvar": {}, "topk": {}, "bottomk": {}, "quantile": {},
	"count_values": {}, "by": {}, "without": {}, "and": {}, "or": {},
	"unless": {}, "on": {}, "ignoring": {}, "group_left": {}, "group_right": {},
	"offset": {}, "bool": {}, "inf": {}, "nan": {}, "le": {},
}

func TestDashboardTargets(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "deploy", "grafana", "collab.json"))
	if err != nil {
		t.Fatal(err)
	}
	var dash struct {
		Panels []struct {
			Title      string `json:"title"`
			Datasource *struct {
				UID string `json:"uid"`
			} `json:"datasource"`
			Targets []struct {
				Expr       string `json:"expr"`
				Datasource *struct {
					UID string `json:"uid"`
				} `json:"datasource"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatal(err)
	}
	if len(dash.Panels) == 0 {
		t.Fatal("dashboard has no panels")
	}
	for _, panel := range dash.Panels {
		if panel.Datasource == nil || panel.Datasource.UID != "prometheus" {
			t.Errorf("%s datasource uid %v", panel.Title, panel.Datasource)
		}
		if len(panel.Targets) == 0 {
			t.Errorf("%s has no targets", panel.Title)
		}
		for _, target := range panel.Targets {
			if target.Datasource == nil || target.Datasource.UID != "prometheus" {
				t.Errorf("%s target datasource %v", panel.Title, target.Datasource)
			}
			names := exprMetrics(target.Expr)
			if len(names) == 0 {
				t.Errorf("%s expression %q names no metric", panel.Title, target.Expr)
			}
			for _, name := range names {
				if !allowedMetric(name) {
					t.Errorf("%s expression %q names %s", panel.Title, target.Expr, name)
				}
			}
		}
	}

	cfgRaw, err := os.ReadFile(filepath.Join(root, "deploy", "prometheus", "prometheus.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		ScrapeConfigs []struct {
			StaticConfigs []struct {
				Targets []string `yaml:"targets"`
			} `yaml:"static_configs"`
		} `yaml:"scrape_configs"`
	}
	if err := yaml.Unmarshal(cfgRaw, &cfg); err != nil {
		t.Fatal(err)
	}
	var targets []string
	for _, scrape := range cfg.ScrapeConfigs {
		for _, static := range scrape.StaticConfigs {
			targets = append(targets, static.Targets...)
		}
	}
	for _, port := range []string{"9190", "9191", "9192", "9193", "9194", "9195", "9196"} {
		found := false
		for _, target := range targets {
			if strings.HasSuffix(target, ":"+port) {
				found = true
			}
		}
		if !found {
			t.Errorf("prometheus.yml missing port %s in %v", port, targets)
		}
	}
}

func allowedMetric(name string) bool {
	if strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_") {
		return true
	}
	base := name
	for _, suffix := range []string{"_bucket", "_count", "_sum"} {
		if strings.HasSuffix(base, suffix) {
			base = strings.TrimSuffix(base, suffix)
			break
		}
	}
	_, ok := metricNames[base]
	return ok
}

func exprMetrics(expr string) []string {
	var names []string
	for i := 0; i < len(expr); {
		switch expr[i] {
		case '"', '\'':
			quote := expr[i]
			i++
			for i < len(expr) && expr[i] != quote {
				if expr[i] == '\\' && i+1 < len(expr) {
					i += 2
					continue
				}
				i++
			}
			if i < len(expr) {
				i++
			}
		case '{', '[':
			end := byte('}')
			if expr[i] == '[' {
				end = ']'
			}
			for i < len(expr) && expr[i] != end {
				i++
			}
			if i < len(expr) {
				i++
			}
		default:
			if i > 0 && unicode.IsDigit(rune(expr[i-1])) && unicode.IsLetter(rune(expr[i])) {
				for i < len(expr) && unicode.IsLetter(rune(expr[i])) {
					i++
				}
				continue
			}
			if expr[i] != '_' && !unicode.IsLetter(rune(expr[i])) {
				i++
				continue
			}
			j := i + 1
			for j < len(expr) && (expr[j] == '_' || unicode.IsLetter(rune(expr[j])) || unicode.IsDigit(rune(expr[j]))) {
				j++
			}
			word := expr[i:j]
			k := j
			for k < len(expr) && expr[k] == ' ' {
				k++
			}
			if _, skip := promQLWords[word]; !skip && (k >= len(expr) || expr[k] != '(') {
				names = append(names, word)
			}
			i = j
		}
	}
	return names
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	return filepath.Dir(filepath.Dir(filepath.Dir(file)))
}
