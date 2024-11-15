package pcache

import (
	"testing"
)

func TestOIDC_isTargetNamespacedKey(t *testing.T) {
	tests := []struct {
		nsTargets []string
		nskey     string
		expected  bool
	}{
		{[]string{"nsid"}, "v0:nsid:key", true},
		{[]string{"nsid"}, "v0:nsid:", true},
		{[]string{"nsid"}, "v0:nsid", false},
		{[]string{"nsid"}, "v0:", false},
		{[]string{"nsid"}, "v0", false},
		{[]string{"nsid"}, "", false},
		{[]string{"nsid1"}, "v0:nsid2:key", false},
		{[]string{"nsid1"}, "nsid1:nsid2:nsid1", false},
		{[]string{"nsid1"}, "nsid1:nsid1:nsid1", true},
		{[]string{"nsid"}, "nsid:nsid:nsid:nsid:nsid:nsid", true},
		{[]string{"nsid"}, ":::", false},
		{[]string{""}, ":::", true}, // "" is a valid key for cache.Set/Get
		{[]string{"nsid1"}, "nsid0:nsid1:nsid0:nsid1:nsid0:nsid1", true},
		{[]string{"nsid0"}, "nsid0:nsid1:nsid0:nsid1:nsid0:nsid1", false},
		{[]string{"nsid0", "nsid1"}, "v0:nsid2:key", false},
		{[]string{"nsid0", "nsid1", "nsid2", "nsid3", "nsid4"}, "v0:nsid3:key", true},
		{[]string{"nsid0", "nsid1", "nsid2", "nsid3", "nsid4"}, "nsid0:nsid1:nsid2:nsid3:nsid4:nsid5", true},
		{[]string{"nsid0", "nsid1", "nsid2", "nsid3", "nsid4"}, "nsid4:nsid5:nsid6:nsid7:nsid8:nsid9", false},
		{[]string{"nsid0", "nsid0", "nsid0", "nsid0", "nsid0"}, "nsid0:nsid0:nsid0:nsid0:nsid0:nsid0", true},
		{[]string{"nsid1", "nsid1", "nsid2", "nsid2"}, "nsid0:nsid0:nsid0:nsid0:nsid0:nsid0", false},
		{[]string{"nsid1", "nsid1", "nsid2", "nsid2"}, "nsid0:nsid0:nsid0:nsid0:nsid0:nsid0", false},
	}

	for _, test := range tests {
		actual := IsTargetNamespacedKey(test.nskey, test.nsTargets)
		if test.expected != actual {
			t.Fatalf("expected %t but got %t for nstargets: %q and nskey: %q", test.expected, actual, test.nsTargets, test.nskey)
		}
	}
}
