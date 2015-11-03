// Copyright © 2014 Terry Mao, LiuDing All rights reserved.
// This file is part of gopush-cluster.

// gopush-cluster is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// gopush-cluster is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with gopush-cluster.  If not, see <http://www.gnu.org/licenses/>.

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
