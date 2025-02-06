/*
Copyright 2025 The Vitess Authors.

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

// You can modify this file to hook up a different logging library instead of glog.
// If you adapt to a different logging framework, you may need to use that
// framework's equivalent of *Depth() functions so the file and line number printed
// point to the real caller instead of your adapter function.

package log

import (
	"github.com/spf13/pflag"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	tabletStringerName string = "alias"
	tabletStringers    map[string]tabletStringer
)

type tabletStringer interface {
	TabletString(*topodatapb.Tablet) string
}

func RegisterTabletStringer(name string, ts tabletStringer) {
	if _, found := tabletStringers[name]; !found {
		tabletStringers[name] = ts
	}
}

func TabletString(t *topodatapb.Tablet) string {
	if tabletStringerImpl, found := tabletStringers[tabletStringerName]; found {
		return tabletStringerImpl.TabletString(t)
	}
	return ""
}

type aliasTabletStringer struct{}

func (ts aliasTabletStringer) TabletString(t *topodatapb.Tablet) string {
	if alias := t.GetAlias(); alias != nil {
		return alias.String()
	}
	return ""
}

type aliasHostnameTabletStringer struct{}

func (ts aliasHostnameTabletStringer) TabletString(t *topodatapb.Tablet) string {
	if alias := t.GetAlias(); alias != nil {
		return alias.String() + "/" + t.GetHostname()
	}
	return t.GetHostname()
}

func init() {
	servenv.OnParseFor("vtorc", func(fs *pflag.FlagSet) {
		fs.StringVar("log-tablet", &tabletStringerName, "style of tablet logging")
	})
}
