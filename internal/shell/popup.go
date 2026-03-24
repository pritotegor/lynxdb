package shell

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/lynxbase/lynxdb/internal/ui"
)

const popupMaxVisible = 8

// AutocompletePopup renders a dropdown completion menu.
type AutocompletePopup struct {
	visible      bool
	items        []CompletionItem
	selected     int
	scrollOffset int
	anchorCol    int // cursor column for positioning
}

// Show displays the popup with the given items.
func (p *AutocompletePopup) Show(items []CompletionItem, anchorCol int) {
	if len(items) == 0 {
		p.Hide()
		return
	}

	p.visible = true
	p.items = items
	p.selected = 0
	p.scrollOffset = 0
	p.anchorCol = anchorCol
}

// Hide dismisses the popup.
func (p *AutocompletePopup) Hide() {
	p.visible = false
	p.items = nil
	p.selected = 0
	p.scrollOffset = 0
}

// Visible reports whether the popup is shown.
func (p *AutocompletePopup) Visible() bool {
	return p.visible
}

// SelectedItem returns the currently highlighted item, or nil if hidden.
func (p *AutocompletePopup) SelectedItem() *CompletionItem {
	if !p.visible || len(p.items) == 0 {
		return nil
	}

	return &p.items[p.selected]
}

// MoveDown moves the selection down, scrolling if needed.
func (p *AutocompletePopup) MoveDown() {
	if !p.visible || len(p.items) == 0 {
		return
	}

	p.selected++
	if p.selected >= len(p.items) {
		p.selected = 0
		p.scrollOffset = 0
	}

	p.ensureVisible()
}

// MoveUp moves the selection up, scrolling if needed.
func (p *AutocompletePopup) MoveUp() {
	if !p.visible || len(p.items) == 0 {
		return
	}

	p.selected--
	if p.selected < 0 {
		p.selected = len(p.items) - 1
	}

	p.ensureVisible()
}

func (p *AutocompletePopup) ensureVisible() {
	maxVisible := popupMaxVisible
	if len(p.items) < maxVisible {
		maxVisible = len(p.items)
	}

	if p.selected < p.scrollOffset {
		p.scrollOffset = p.selected
	}

	if p.selected >= p.scrollOffset+maxVisible {
		p.scrollOffset = p.selected - maxVisible + 1
	}
}

// View renders the popup as a string block.
func (p *AutocompletePopup) View(maxWidth int) string {
	if !p.visible || len(p.items) == 0 {
		return ""
	}

	visible := popupMaxVisible
	if len(p.items) < visible {
		visible = len(p.items)
	}

	// Determine column widths.
	maxTextW := 0
	maxKindW := 0
	for _, item := range p.items {
		if len(item.Text) > maxTextW {
			maxTextW = len(item.Text)
		}
		if kl := len(item.Kind.kindLabel()); kl > maxKindW {
			maxKindW = kl
		}
	}

	// Constrain to available width.
	contentW := maxTextW + 2 + maxKindW // text + gap + kind
	if contentW > maxWidth-4 {          // borders + padding
		contentW = maxWidth - 4
		maxTextW = contentW - 2 - maxKindW
		if maxTextW < 6 {
			maxTextW = 6
		}
	}

	selectedStyle := lipgloss.NewStyle().
		Background(ui.ColorInfo()).
		Foreground(lipgloss.Color("#000000")).
		Bold(true)
	normalStyle := lipgloss.NewStyle()
	kindStyle := lipgloss.NewStyle().Foreground(ui.ColorDim())

	var lines []string

	end := p.scrollOffset + visible
	if end > len(p.items) {
		end = len(p.items)
	}

	for i := p.scrollOffset; i < end; i++ {
		item := p.items[i]
		text := truncateStr(item.Text, maxTextW)
		kind := item.Kind.kindLabel()

		// Pad text to fixed width.
		padded := fmt.Sprintf("%-*s", maxTextW, text)

		if i == p.selected {
			entry := fmt.Sprintf(" %s  %s ", padded, kind)
			lines = append(lines, selectedStyle.Render(entry))
		} else {
			entry := fmt.Sprintf(" %s  %s ", padded, kindStyle.Render(kind))
			lines = append(lines, normalStyle.Render(entry))
		}
	}

	// Show scroll indicator if items exceed visible.
	if len(p.items) > visible {
		indicator := fmt.Sprintf(" %d/%d", p.selected+1, len(p.items))
		lines = append(lines, kindStyle.Render(indicator))
	}

	content := strings.Join(lines, "\n")

	border := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(ui.ColorDim())

	return border.Render(content)
}
