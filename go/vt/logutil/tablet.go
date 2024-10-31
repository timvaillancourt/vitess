package logutil

import (
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	tabletFormatter     TabletFormatter = &AliasTabletFormatter{}
	tabletFormatterName string          = "alias"
)

type TabletFormatter interface {
	toString(tablet *topodatapb.Tablet) string
}

type AliasTabletFormatter struct{}

func (tf *AliasTabletFormatter) toString(tablet *topodatapb.Tablet) string {
	return topoproto.TabletAliasString(tablet.Alias)
}

type AliasHostnameTabletFormatter struct{}

func (tf *AliasHostnameTabletFormatter) toString(tablet *topodatapb.Tablet) string {
	str := topoproto.TabletAliasString(tablet.Alias)
	if tablet.Hostname == "" {
		return str
	}
	return str + "/" + tablet.Hostname
}

func setTabletFormatter(name string) {
	switch tabletFormatterName {
	case "aliasHostname":
		tabletFormatter = &AliasHostnameTabletFormatter{}
	default:
		tabletFormatter = &AliasTabletFormatter{}
	}
}

func TabletToString(tablet *topodatapb.Tablet) string {
	return tabletFormatter.toString(tablet)
}

func init() {
	setTabletFormatter(tabletFormatterName)
}
