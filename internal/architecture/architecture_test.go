package architecture_test

// The layering is part of the design (high cohesion, low coupling), so it is
// enforced rather than hoped for. A new import that breaks a rule fails here
// with the offending package and import named.
//
//	pkg/sdk          the public contract: standard library only
//	internal/rules   pure logic: no database, queue, engine or network
//	internal/store   database plumbing: depends on nothing else of ours
//	internal/queue   may use store (SQL) and sdk; knows nothing of the engine
//	internal/engine  the pipeline: no SQL, no config, no concrete sinks
//	store/sqlite     implements the engine's ports: engine + store + sdk only
//	internal/sinks   destinations: sdk only
//	internal/config  rules + sinks only
//	internal/api     the HTTP interface: engine + sdk only; no SQL
//	internal/app     the composition root: may import anything, and nothing imports it

import (
	"bufio"
	"go/build"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func modulePath(t *testing.T) string {
	t.Helper()
	f, err := os.Open("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(sc.Text()), "module "); ok {
			return strings.TrimSpace(rest)
		}
	}
	t.Fatal("no module line in go.mod")
	return ""
}

// imports returns the non-test imports of the package in dir (relative to the repo root).
func imports(t *testing.T, dir string) []string {
	t.Helper()
	pkg, err := build.ImportDir(filepath.Join("../..", dir), 0)
	if err != nil {
		t.Fatalf("%s: %v", dir, err)
	}
	return pkg.Imports
}

type rule struct {
	pkg string
	// allowedInternal lists the packages of this module it may import (relative paths).
	allowedInternal []string
	// allowedExternal lists third-party import prefixes it may use; the standard library is always allowed.
	allowedExternal []string
	// forbiddenStd lists standard-library packages it must not touch.
	forbiddenStd []string
}

var rules = []rule{
	{pkg: "pkg/sdk", forbiddenStd: []string{"database/sql", "net/http"}},
	{pkg: "internal/rules", allowedInternal: []string{"pkg/sdk"}, allowedExternal: []string{"github.com/expr-lang/expr"},
		forbiddenStd: []string{"database/sql", "net/http"}},
	{pkg: "internal/store", allowedExternal: []string{"modernc.org/sqlite", "github.com/mattn/go-sqlite3"}},
	{pkg: "internal/queue", allowedInternal: []string{"internal/store", "pkg/sdk"}, allowedExternal: []string{"github.com/oklog/ulid"},
		forbiddenStd: []string{"net/http"}},
	{pkg: "internal/engine", allowedInternal: []string{"internal/queue", "internal/rules", "pkg/sdk"}, allowedExternal: []string{"github.com/oklog/ulid"},
		forbiddenStd: []string{"database/sql", "net/http"}},
	{pkg: "internal/store/sqlite", allowedInternal: []string{"internal/engine", "internal/store", "pkg/sdk"}},
	{pkg: "internal/sinks", allowedInternal: []string{"pkg/sdk"}, forbiddenStd: []string{"database/sql"}},
	{pkg: "internal/config", allowedInternal: []string{"internal/rules", "internal/sinks"}, allowedExternal: []string{"github.com/goccy/go-yaml"},
		forbiddenStd: []string{"database/sql", "net/http"}},
	{pkg: "internal/api", allowedInternal: []string{"internal/engine", "pkg/sdk"}, forbiddenStd: []string{"database/sql"}},
}

func TestEachPackageImportsOnlyWhatItsLayerAllows(t *testing.T) {
	mod := modulePath(t)
	for _, r := range rules {
		allowed := map[string]bool{}
		for _, a := range r.allowedInternal {
			allowed[mod+"/"+a] = true
		}
		for _, imp := range imports(t, r.pkg) {
			switch {
			case strings.HasPrefix(imp, mod+"/"):
				if !allowed[imp] {
					t.Errorf("%s must not import %s (allowed: %v)", r.pkg, strings.TrimPrefix(imp, mod+"/"), r.allowedInternal)
				}
			case isStd(imp):
				for _, bad := range r.forbiddenStd {
					if imp == bad {
						t.Errorf("%s must not import %s: this layer knows nothing about it", r.pkg, imp)
					}
				}
			default:
				ok := false
				for _, prefix := range r.allowedExternal {
					if strings.HasPrefix(imp, prefix) {
						ok = true
					}
				}
				if !ok {
					t.Errorf("%s imports third-party package %s, which is not on its allowlist %v: add it deliberately in architecture_test.go if it is intended",
						r.pkg, imp, r.allowedExternal)
				}
			}
		}
	}
}

func TestNothingOutsideTheCompositionRootImportsIt(t *testing.T) {
	mod := modulePath(t)
	for _, r := range rules {
		for _, imp := range imports(t, r.pkg) {
			if imp == mod+"/internal/app" {
				t.Errorf("%s imports internal/app; only commands may depend on the composition root", r.pkg)
			}
		}
	}
}

func isStd(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}

// Every package in the module is covered by a rule (or is a known exception), so a
// new package cannot slip in unconstrained.
func TestEveryPackageIsGovernedByARule(t *testing.T) {
	covered := map[string]bool{"internal/app": true, "internal/architecture": true,
		"internal/queue/queuetest": true, "internal/engine/storetest": true, "internal/sinks/sinktest": true}
	for _, r := range rules {
		covered[r.pkg] = true
	}
	var found []string
	_ = filepath.WalkDir("../..", func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel("../..", path)
		if strings.HasPrefix(rel, ".") && rel != "." {
			return filepath.SkipDir
		}
		if pkg, err := build.ImportDir(path, 0); err == nil && len(pkg.GoFiles) > 0 {
			found = append(found, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(found)
	for _, p := range found {
		if !covered[p] && !strings.HasPrefix(p, "cmd/") && !strings.HasPrefix(p, "test/") {
			t.Errorf("package %s has no dependency rule: add one to architecture_test.go", p)
		}
	}
}

// Test-support packages may import their subject, but production code must never import them.
func TestProductionCodeNeverImportsTestSupportPackages(t *testing.T) {
	mod := modulePath(t)
	support := []string{"internal/queue/queuetest", "internal/engine/storetest", "internal/sinks/sinktest"}
	for _, r := range rules {
		for _, imp := range imports(t, r.pkg) {
			for _, s := range support {
				if imp == mod+"/"+s {
					t.Errorf("%s imports test-support package %s", r.pkg, s)
				}
			}
		}
	}
}
