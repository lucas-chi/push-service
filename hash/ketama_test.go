// refer to https://github.com/mncaudill/ketama

package hash

import (
	"testing"
)

func TestKetama(t *testing.T) {
	k := NewKetama(15, 255)
	n := k.Node("lucas-chi332")
	if n != "node15" {
		t.Error("lucas-chi332 must hit node13")
	}

	n = k.Node("lucas-chi2")
	if n != "node13" {
		t.Error("lucas-chi2 must hit node13")
	}

	n = k.Node("lucas-chi3")
	if n != "node5" {
		t.Error("lucas-chi3 must hit node13")
	}

	n = k.Node("lucas-chi4")
	if n != "node2" {
		t.Error("lucas-chi4 must hit node13")
	}

	n = k.Node("lucas-chi5")
	if n != "node6" {
		t.Error("lucas-chi5 must hit node13")
	}
}

func TestKetama2(t *testing.T) {
	k := NewKetama2([]string{"11", "22"}, 255)
	n := k.Node("lucas-chi332")
	if n != "22" {
		t.Error("lucas-chi332 must hit 22")
	}

	n = k.Node("lucas-chi333")
	if n != "22" {
		t.Error("lucas-chi333 must hit 22")
	}

	n = k.Node("lucas-chi334")
	if n != "22" {
		t.Error("lucas-chi333 must hit 22")
	}

	n = k.Node("lucas-chi335")
	if n != "11" {
		t.Error("lucas-chi335 must hit 11")
	}
}
