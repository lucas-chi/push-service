
package process

import (
	"testing"
)

// mkdir -p /tmp/test && chown -R nobody:nobody /tmp/test
// sudo go test
func TestInit(t *testing.T) {
	if err := Init("nobody nobody", "./", "/tmp/test/process_test.pid"); err != nil {
		t.Error(err)
	}
}
