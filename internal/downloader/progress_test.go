package downloader

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestBar(t *testing.T) {
	cases := []struct {
		frac  float64
		width int
		want  string
	}{
		{0, 4, "░░░░"},
		{0.5, 4, "██░░"},
		{1, 4, "████"},
		{-1, 2, "░░"},     // clamped
		{2, 2, "██"},      // clamped
		{0.999, 3, "██░"}, // truncated, not rounded
	}
	for _, c := range cases {
		if got := bar(c.frac, c.width); got != c.want {
			t.Errorf("bar(%v, %d) = %q, want %q", c.frac, c.width, got, c.want)
		}
	}
}

func TestFrameLines(t *testing.T) {
	snap := Snapshot{
		DoneObjects: 2, TotalObjects: 8,
		DoneBytes: 1500, TotalBytes: 3000,
		Elapsed: 90 * time.Second,
		Active: []ObjView{
			{Label: "layer aabbccddeeff", State: "down", Done: 750, Total: 1500, Speed: 1024},
			{Label: "config 112233445566", State: "queue", Done: 0, Total: 37, Speed: 0},
			{Label: "layer ffeeffeedd00", State: "verify", Done: 99, Total: 99, Speed: 0},
		},
	}
	lines := frameLines(snap)
	if len(lines) != 4 {
		t.Fatalf("frameLines produced %d lines, want 4: %q", len(lines), lines)
	}
	head := lines[0]
	for _, want := range []string{"2/8 objects", "50%", "3 active", "3 queued", "1m30s"} {
		if !strings.Contains(head, want) {
			t.Errorf("header %q missing %q", head, want)
		}
	}
	row := lines[1]
	for _, want := range []string{"layer aabbccddeeff", "50%", "750 B", "1.5 KiB", "1.0 KiB/s"} {
		if !strings.Contains(row, want) {
			t.Errorf("row %q missing %q", row, want)
		}
	}
	if got := lines[2]; !strings.Contains(got, "config 112233445566  preparing…") {
		t.Errorf("queue row not rendered as preparing: %q", got)
	}
	if got := lines[3]; !strings.Contains(got, "verifying SHA-256…") {
		t.Errorf("verify row not rendered as verifying: %q", got)
	}

	// Queued count appears when pending objects outnumber visible rows.
	snap.Active = snap.Active[:1]
	if head := frameLines(snap)[0]; !strings.Contains(head, "5 queued") {
		t.Errorf("header %q missing %q", head, "5 queued")
	}
}

func newTestProgress(tty bool) (*Progress, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	p := &Progress{out: buf, tty: tty}
	return p, buf
}

func TestRenderTTYAndClear(t *testing.T) {
	p, buf := newTestProgress(true)
	p.Render(Snapshot{DoneObjects: 0, TotalObjects: 2, Active: []ObjView{
		{Label: "layer aabbccddeeff", Done: 10, Total: 100, Speed: 5},
	}})
	first := buf.String()
	if !strings.Contains(first, "layer aabbccddeeff") {
		t.Fatalf("first frame missing object row: %q", first)
	}
	if got := strings.Count(first, "\n"); got != 2 {
		t.Errorf("first frame has %d lines, want 2", got)
	}

	// Second render with one fewer row must erase the previous frame first.
	buf.Reset()
	p.Render(Snapshot{DoneObjects: 2, TotalObjects: 2})
	second := buf.String()
	if n := strings.Count(second, "\033[1A\033[2K"); n != 2 {
		t.Errorf("clear sequence count = %d, want 2 (one per drawn row)", n)
	}
	if !strings.Contains(second, "2/2 objects") {
		t.Errorf("second frame missing header: %q", second)
	}
}

func TestRenderNonTTYIsNoop(t *testing.T) {
	p, buf := newTestProgress(false)
	p.Render(Snapshot{DoneObjects: 1, TotalObjects: 1, Active: []ObjView{{Label: "layer x", Done: 1, Total: 1}}})
	if buf.Len() != 0 {
		t.Errorf("non-TTY Render wrote %q, want nothing", buf.String())
	}
	if p.Interactive() {
		t.Error("non-TTY progress reported Interactive")
	}
}

func TestLogfTTYClearsPanel(t *testing.T) {
	p, buf := newTestProgress(true)
	p.Render(Snapshot{DoneObjects: 0, TotalObjects: 1, Active: []ObjView{{Label: "layer x", Done: 1, Total: 9}}})
	buf.Reset()
	p.Logf("✓ layer x (9 B) [1/1]")
	out := buf.String()
	if n := strings.Count(out, "\033[1A\033[2K"); n != 2 {
		t.Errorf("Logf cleared %d rows, want 2", n)
	}
	if !strings.Contains(out, "imgpull: ✓ layer x") {
		t.Errorf("Logf output %q missing prefix/event", out)
	}
}
