package downloader

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"imgpull/internal/fsx"
)

// ObjView is one in-flight object row in the progress panel.
type ObjView struct {
	Label string // e.g. "layer 8f3a1b2c3d4e"
	State string // "", "queue" (preparing), "down", "verify"
	Done  int64
	Total int64
	Speed int64 // bytes per second
}

// Snapshot is a point-in-time view of the whole download.
type Snapshot struct {
	DoneObjects  int
	TotalObjects int
	DoneBytes    int64
	TotalBytes   int64
	Active       []ObjView
	Elapsed      time.Duration
}

// Progress renders a live progress panel on stderr and interleaves event
// lines (commits, retries, warnings) without tearing them apart. When
// stderr is not a terminal it degrades to plain log lines.
type Progress struct {
	mu    sync.Mutex
	out   io.Writer
	tty   bool
	drawn int // panel rows currently on screen
}

// NewProgress builds a renderer attached to stderr, detecting interactivity.
func NewProgress() *Progress {
	p := &Progress{out: os.Stderr}
	if f, ok := p.out.(*os.File); ok {
		if fi, err := f.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
			p.tty = true
		}
	}
	return p
}

// Interactive reports whether the panel mode (dynamic redraw) is active.
func (p *Progress) Interactive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tty
}

// Logf prints an event line, erasing the panel first when interactive.
func (p *Progress) Logf(format string, args ...interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	msg := fmt.Sprintf(format, args...)
	if p.tty {
		p.clearLocked()
		fmt.Fprintf(p.out, "imgpull: %s\n", msg)
		return
	}
	// log package carries the "imgpull: " prefix.
	log.Print(msg)
}

// Render redraws the whole panel from snap. No-op on a non-TTY.
func (p *Progress) Render(snap Snapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.tty {
		return
	}
	p.clearLocked()
	for _, l := range frameLines(snap) {
		fmt.Fprintln(p.out, l)
		p.drawn++
	}
}

// clearLocked erases the currently drawn panel rows (TTY only).
func (p *Progress) clearLocked() {
	for i := 0; i < p.drawn; i++ {
		fmt.Fprint(p.out, "\033[1A\033[2K")
	}
	p.drawn = 0
}

// frameLines builds the panel rows: aggregate header + one row per active
// object. Rows are kept short enough to avoid terminal line wrapping.
func frameLines(snap Snapshot) []string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d/%d objects | %s / %s",
		snap.DoneObjects, snap.TotalObjects,
		fsx.HumanSize(snap.DoneBytes), fsx.HumanSize(snap.TotalBytes))
	if snap.TotalBytes > 0 && snap.DoneBytes <= snap.TotalBytes {
		fmt.Fprintf(&b, " (%d%%)", int(100*snap.DoneBytes/snap.TotalBytes))
	}
	if sp := sumSpeed(snap.Active); sp > 0 {
		fmt.Fprintf(&b, " | %s/s", fsx.HumanSize(sp))
	}
	fmt.Fprintf(&b, " | %d active", len(snap.Active))
	if queued := snap.TotalObjects - snap.DoneObjects - len(snap.Active); queued > 0 {
		fmt.Fprintf(&b, " | %d queued", queued)
	}
	if snap.Elapsed > 0 {
		fmt.Fprintf(&b, " | %s", snap.Elapsed.Truncate(time.Second))
	}
	lines := []string{b.String()}
	for _, o := range snap.Active {
		lines = append(lines, objLine(o))
	}
	return lines
}

func objLine(o ObjView) string {
	var sb strings.Builder
	sb.WriteString(o.Label)
	sb.WriteString("  ")
	switch o.State {
	case "queue":
		sb.WriteString("preparing… (resolving URL / waiting for aria2)")
		return sb.String()
	case "verify":
		fmt.Fprintf(&sb, "[%s] 100%%  %s/%s  verifying SHA-256…",
			bar(1, 20), fsx.HumanSize(o.Total), fsx.HumanSize(o.Total))
		return sb.String()
	}
	if o.Total > 0 {
		frac := float64(o.Done) / float64(o.Total)
		fmt.Fprintf(&sb, "[%s] %3.0f%%  %s/%s", bar(frac, 20), frac*100,
			fsx.HumanSize(o.Done), fsx.HumanSize(o.Total))
	} else {
		sb.WriteString(fsx.HumanSize(o.Done))
	}
	if o.Speed > 0 {
		fmt.Fprintf(&sb, "  %s/s", fsx.HumanSize(o.Speed))
	}
	return sb.String()
}

// bar renders a fraction as a filled/empty Unicode block strip.
func bar(frac float64, width int) string {
	switch {
	case frac < 0:
		frac = 0
	case frac > 1:
		frac = 1
	}
	full := int(frac * float64(width))
	return strings.Repeat("█", full) + strings.Repeat("░", width-full)
}

func sumSpeed(objs []ObjView) int64 {
	var n int64
	for _, o := range objs {
		n += o.Speed
	}
	return n
}

// renderProgress snapshots scheduler state and either redraws the TTY panel
// or, on a pipe, logs one aggregate line at the configured cadence.
func (s *Scheduler) renderProgress() {
	snap := s.snapshot()
	if s.prog.Interactive() {
		s.prog.Render(snap)
		return
	}
	if len(snap.Active) == 0 {
		return
	}
	log.Printf("progress %d/%d files committed | %s / %s | %s/s | %d active",
		snap.DoneObjects, snap.TotalObjects,
		fsx.HumanSize(snap.DoneBytes), fsx.HumanSize(snap.TotalBytes),
		fsx.HumanSize(sumSpeed(snap.Active)), len(snap.Active))
}

// snapshot collects committed counts, per-object progress and aggregate
// bytes for the progress display.
func (s *Scheduler) snapshot() Snapshot {
	snap := Snapshot{
		TotalObjects: len(s.plan.Objects),
		Elapsed:      time.Since(s.started),
	}
	snap.DoneObjects = snap.TotalObjects - len(s.plan.PendingObjects())
	s.mu.Lock()
	for digest, li := range s.live {
		snap.Active = append(snap.Active, ObjView{
			Label: fmt.Sprintf("%s %s", li.kind, short(digest)),
			State: li.state,
			Done:  li.done, Total: li.total, Speed: li.speed,
		})
	}
	s.mu.Unlock()
	sort.Slice(snap.Active, func(i, j int) bool { return snap.Active[i].Label < snap.Active[j].Label })
	committed := s.plan.DoneBytes()
	snap.DoneBytes = committed
	for _, o := range snap.Active {
		snap.DoneBytes += o.Done
	}
	// Grand total = committed sizes + remaining sizes (including in-flight),
	// so the header still shows the full picture once everything is done.
	snap.TotalBytes = committed + s.plan.TotalBytes()
	return snap
}
