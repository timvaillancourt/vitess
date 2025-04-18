/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package codegen

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path"
	"reflect"
	"sort"
	"time"

	"vitess.io/vitess/go/tools/codegen"
)

type Generator struct {
	bytes.Buffer
	local    Package
	imported map[Package]bool
}

func NewGenerator(pkg Package) *Generator {
	return &Generator{
		local:    pkg,
		imported: make(map[Package]bool),
	}
}

func Merge(gens ...*Generator) *Generator {
	result := NewGenerator(gens[0].local)

	for i, gen := range gens {
		if gen.local != result.local {
			result.Fail("cannot merge generators with different package names")
		}

		for pkg, imported := range gen.imported {
			if !result.imported[pkg] {
				result.imported[pkg] = imported
			}
		}

		if i > 0 {
			result.WriteString("\n\n")
		}
		gen.WriteTo(result)
	}

	return result
}

const licenseFileHeader = `/*
Copyright %d The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

`

func (g *Generator) WriteToFile(out string) {
	var file, fmtfile bytes.Buffer
	file.Grow(g.Len() + 1024)

	fmt.Fprintf(&file, licenseFileHeader, time.Now().Year())
	fmt.Fprintf(&file, "// Code generated by %s DO NOT EDIT\n\n", path.Base(os.Args[0]))
	fmt.Fprintf(&file, "package %s\n\n", g.local.Name())
	fmt.Fprintf(&file, "import (\n")

	var sortedPackages []Package
	for pkg := range g.imported {
		sortedPackages = append(sortedPackages, pkg)
	}
	sort.Slice(sortedPackages, func(i, j int) bool {
		return sortedPackages[i] < sortedPackages[j]
	})
	for _, pkg := range sortedPackages {
		var name = "_"
		if g.imported[pkg] {
			name = pkg.Name()
		}
		fmt.Fprintf(&file, "%s %q\n", name, pkg)
	}

	fmt.Fprintf(&file, ")\n\n")
	g.Buffer.WriteTo(&file)

	if err := os.WriteFile(out, file.Bytes(), 0644); err != nil {
		g.Fail(fmt.Sprintf("failed to generate %q: %v", out, err))
	}

	if err := codegen.GoImports(out); err != nil {
		g.Fail(err.Error())
	}

	log.Printf("written %q (%.02fkb)", out, float64(fmtfile.Len())/1024.0)
}

func (g *Generator) Fail(err string) {
	log.Printf("codegen: error: %v", err)
	os.Exit(1)
}

func (g *Generator) printArray(iface any) {
	switch ary := iface.(type) {
	case Array8:
		g.WriteString("[...]uint8{")
		for i, v := range ary {
			if i%16 == 0 && len(ary)%16 == 0 {
				g.WriteByte('\n')
			}
			fmt.Fprintf(g, "0x%02x, ", v)
		}
	case Array16:
		g.WriteString("[...]uint16{")
		for i, v := range ary {
			if i%8 == 0 && len(ary)%8 == 0 {
				g.WriteByte('\n')
			}
			fmt.Fprintf(g, "0x%04x, ", v)
		}
	case Array32:
		g.WriteString("[...]uint32{")
		for i, v := range ary {
			if i%8 == 0 && len(ary)%8 == 0 {
				g.WriteByte('\n')
			}
			fmt.Fprintf(g, "0x%08x, ", v)
		}
	}
	g.WriteString("\n}")
}

func (g *Generator) gatherPackages(tt reflect.Type) {
	if pkg := tt.PkgPath(); pkg != "" {
		g.imported[Package(pkg)] = true
	}
	switch tt.Kind() {
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Ptr, reflect.Slice:
		g.gatherPackages(tt.Elem())
	case reflect.Struct:
		for i := 0; i < tt.NumField(); i++ {
			g.gatherPackages(tt.Field(i).Type)
		}
	}
}

func (g *Generator) UsePackage(pkg Package) {
	g.imported[pkg] = false
}

func (g *Generator) printAtom(v any) {
	switch v := v.(type) {
	case string:
		g.WriteString(v)
	case *string:
		g.WriteString(*v)
	case bool:
		fmt.Fprint(g, v)
	case *bool:
		fmt.Fprint(g, *v)
	case int:
		fmt.Fprint(g, v)
	case *int32:
		fmt.Fprint(g, *v)
	case *int64:
		fmt.Fprint(g, *v)
	case float64:
		fmt.Fprint(g, v)
	case *float64:
		fmt.Fprint(g, *v)
	case Package:
		g.imported[v] = true
		g.WriteString(v.Name())
	case Quote:
		fmt.Fprintf(g, "%q", v)
	case Array8, Array16, Array32:
		g.printArray(v)
	default:
		g.gatherPackages(reflect.TypeOf(v))
		fmt.Fprintf(g, "%#v", v)
	}
}

type Array8 []byte
type Array16 []uint16
type Array32 []uint32
type Quote string
type Package string

func (pkg Package) Name() string {
	return path.Base(string(pkg))
}

func (g *Generator) P(str ...any) {
	for _, v := range str {
		g.printAtom(v)
	}
	g.WriteByte('\n')
}
