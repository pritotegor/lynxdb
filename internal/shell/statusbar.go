package shell

import (
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/spinner"
	"charm.land/lipgloss/v2"

	"github.com/lynxbase/lynxdb/internal/ui"
)

// Focus indicates which component has keyboard focus.
type Focus int

const (
	EditorFocus Focus = iota
	ResultsFocus
)

// StatusBar renders the bottom shortcut bar with context-dependent hints.
type StatusBar struct {
	spinner spinner.Model
	width   int
	mode    string

	// Transient flash message (e.g. "Copied!"), cleared after flashUntil.
	flashMsg   string
	flashUntil time.Time
}

// NewStatusBar creates a status bar.
func NewStatusBar(mode string) StatusBar {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = ui.Stdout.Accent

	return StatusBar{
		spinner: s,
		width:   80,
		mode:    mode,
	}
}

// SetWidth updates the status bar width.
func (sb *StatusBar) SetWidth(w int) {
	sb.width = w
}

// SetFlash sets a transient message that displays for the given duration.
func (sb *StatusBar) SetFlash(msg string, d time.Duration) {
	sb.flashMsg = msg
	sb.flashUntil = time.Now().Add(d)
}

// View renders the status bar based on current state.
func (sb StatusBar) View(focus Focus, running bool, inMulti bool, elapsed time.Duration, progress *progressMsg, tailActive bool, sidebarOpen bool) string {
	style := lipgloss.NewStyle().
		Width(sb.width).
		Foreground(ui.ColorDim()).
		PaddingLeft(1)

	// Flash message takes priority.
	if sb.flashMsg != "" && time.Now().Before(sb.flashUntil) {
		return style.Render(sb.flashMsg)
	}

	var content string

	switch {
	case tailActive:
		content = fmt.Sprintf("%s Live tail active    %s",
			sb.spinner.View(),
			shortcut("Ctrl+C", "stop"))

	case running && progress != nil && progress.segmentsTotal > 0:
		el := elapsed.Round(10 * time.Millisecond)
		content = fmt.Sprintf("%s %s  %s/%s segments  %s    %s",
			sb.spinner.View(),
			phaseDisplayName(progress.phase),
			formatCountShell(int64(progress.segmentsScanned)),
			formatCountShell(int64(progress.segmentsTotal)),
			ui.Stdout.Value.Render(el.String()),
			shortcut("Ctrl+C", "cancel"))

	case running:
		el := elapsed.Round(10 * time.Millisecond)
		content = fmt.Sprintf("%s Executing... %s    %s",
			sb.spinner.View(),
			ui.Stdout.Value.Render(el.String()),
			shortcut("Ctrl+C", "cancel"))

	case focus == ResultsFocus:
		content = shortcutBar(
			shortcut("j/k", "scroll"),
			shortcut("Esc", "back"),
			shortcut("F2", "sidebar"),
			shortcut("F1", "help"),
		)

	case inMulti:
		content = shortcutBar(
			shortcut("Enter", "run"),
			shortcut("Shift+Enter", "newline"),
			shortcut("Ctrl+C", "clear"),
			shortcut("F2", "sidebar"),
		)

	default:
		content = shortcutBar(
			shortcut("Enter", "run"),
			shortcut("↑↓", "history"),
			shortcut("Tab", "complete"),
			shortcut("Ctrl+E", "editor"),
			shortcut("F2", "sidebar"),
			shortcut("F1", "help"),
		)
	}

	return style.Render(content)
}

// shortcut renders a single key:action pair with styled key.
func shortcut(key, action string) string {
	keyStyle := lipgloss.NewStyle().Foreground(ui.ColorGray()).Bold(true)
	return keyStyle.Render(key) + ":" + action
}

// shortcutBar joins shortcut pairs with consistent spacing.
func shortcutBar(items ...string) string {
	return strings.Join(items, "  ")
}

// phaseDisplayName maps a query execution phase to a user-friendly label.
func phaseDisplayName(phase string) string {
	switch phase {
	case "parsing":
		return "Parsing query..."
	case "scanning_buffer":
		return "Scanning buffer..."
	case "filtering_segments":
		return "Filtering segments..."
	case "scanning_segments":
		return "Scanning segments..."
	case "executing_pipeline":
		return "Executing pipeline..."
	default:
		return "Searching..."
	}
}
