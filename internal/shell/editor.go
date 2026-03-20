package shell

import (
	"strings"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/textarea"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

const editorMaxLines = 6

// Editor wraps textarea.Model with history navigation, ghost-text autocomplete,
// and dynamic height.
type Editor struct {
	input      textarea.Model
	history    *History
	completer  *Completer
	prompt     string
	contPrompt string
	keys       keyMap
	ghostText  string // autocomplete ghost suffix (dimmed in View)
}

// NewEditor creates an editor with the given prompt strings.
func NewEditor(prompt, contPrompt string, history *History, completer *Completer) Editor {
	ta := textarea.New()
	ta.ShowLineNumbers = false
	ta.CharLimit = 0
	ta.MaxHeight = editorMaxLines
	ta.SetHeight(1)
	ta.EndOfBufferCharacter = ' '

	// Override textarea KeyMap so our shell-level bindings don't conflict.
	// InsertNewline: only shift+enter (we handle plain enter for submit).
	ta.KeyMap.InsertNewline = key.NewBinding(key.WithKeys("shift+enter"))
	// LinePrevious: only up arrow (we use ctrl+p for history).
	ta.KeyMap.LinePrevious = key.NewBinding(key.WithKeys("up"))
	// LineNext: only down arrow (we use ctrl+n for history).
	ta.KeyMap.LineNext = key.NewBinding(key.WithKeys("down"))

	promptWidth := lipgloss.Width(prompt)
	ta.SetPromptFunc(promptWidth, func(info textarea.PromptInfo) string {
		if info.LineNumber == 0 {
			return prompt
		}
		return contPrompt
	})

	// Minimal styling — no borders, no background, clear CursorLine highlight.
	styles := ta.Styles()
	styles.Focused.CursorLine = lipgloss.NewStyle()
	styles.Focused.Base = lipgloss.NewStyle()
	styles.Blurred.CursorLine = lipgloss.NewStyle()
	styles.Blurred.Base = lipgloss.NewStyle()
	ta.SetStyles(styles)

	ta.Focus()

	return Editor{
		input:      ta,
		history:    history,
		completer:  completer,
		prompt:     prompt,
		contPrompt: contPrompt,
		keys:       defaultKeyMap(),
	}
}

// Value returns the current text input value.
func (e *Editor) Value() string {
	return e.input.Value()
}

// SetWidth updates the input width.
func (e *Editor) SetWidth(w int) {
	e.input.SetWidth(w)
}

// EditorHeight returns the current height of the textarea.
func (e *Editor) EditorHeight() int {
	return e.input.Height()
}

// InMultiLine reports whether the editor content spans multiple lines.
func (e *Editor) InMultiLine() bool {
	return e.input.LineCount() > 1
}

// Update handles key events and returns commands.
func (e *Editor) Update(msg tea.Msg) (tea.Cmd, *querySubmitMsg, *slashCommandMsg) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch {
		case key.Matches(msg, e.keys.Submit):
			return e.handleSubmit()

		case key.Matches(msg, e.keys.InsertNewline):
			e.input.InsertRune('\n')
			e.updateHeight()
			e.refreshSuggestions()
			return nil, nil, nil

		case key.Matches(msg, e.keys.Cancel):
			return e.handleCancel()

		case key.Matches(msg, e.keys.Quit):
			if e.input.Value() == "" {
				return nil, nil, &slashCommandMsg{quit: true}
			}
			// Non-empty: fall through to textarea (ctrl+d = delete forward).

		case key.Matches(msg, e.keys.AcceptSugg):
			if e.ghostText != "" {
				e.input.InsertString(e.ghostText)
				e.ghostText = ""
				e.updateHeight()
			}
			e.refreshSuggestions()
			return nil, nil, nil

		case key.Matches(msg, e.keys.HistPrev):
			if entry, ok := e.history.Prev(); ok {
				e.input.SetValue(entry)
				e.input.MoveToEnd()
				e.updateHeight()
			}
			e.refreshSuggestions()
			return nil, nil, nil

		case key.Matches(msg, e.keys.HistNext):
			if entry, ok := e.history.Next(); ok {
				e.input.SetValue(entry)
				e.input.MoveToEnd()
			} else {
				e.input.Reset()
			}
			e.updateHeight()
			e.refreshSuggestions()
			return nil, nil, nil
		}
	}

	// Default textarea update.
	var cmd tea.Cmd
	e.input, cmd = e.input.Update(msg)

	e.updateHeight()
	e.refreshSuggestions()

	return cmd, nil, nil
}

// refreshSuggestions recomputes ghost text when cursor is at the end of input.
func (e *Editor) refreshSuggestions() {
	if e.completer == nil {
		e.ghostText = ""
		return
	}

	value := e.input.Value()

	// Only suggest when cursor is at the absolute end of input.
	onLastLine := e.input.Line() == e.input.LineCount()-1
	lines := strings.Split(value, "\n")
	lastLine := ""
	if len(lines) > 0 {
		lastLine = lines[len(lines)-1]
	}
	atLineEnd := e.input.Column() >= len(lastLine)

	if !onLastLine || !atLineEnd {
		e.ghostText = ""
		return
	}

	suggestions := e.completer.Suggest(value)
	if len(suggestions) > 0 && len(suggestions[0]) > len(value) {
		e.ghostText = suggestions[0][len(value):]
	} else {
		e.ghostText = ""
	}
}

// updateHeight adjusts textarea height to match content, clamped to [1, editorMaxLines].
func (e *Editor) updateHeight() {
	h := e.input.LineCount()
	if h > editorMaxLines {
		h = editorMaxLines
	}
	if h < 1 {
		h = 1
	}
	e.input.SetHeight(h)
}

func (e *Editor) handleSubmit() (tea.Cmd, *querySubmitMsg, *slashCommandMsg) {
	value := strings.TrimSpace(e.input.Value())
	if value == "" {
		return nil, nil, nil
	}

	// Auto-continue: if the trimmed value ends with |, insert a newline.
	if strings.HasSuffix(value, "|") {
		e.input.InsertRune('\n')
		e.updateHeight()
		return nil, nil, nil
	}

	fullQuery := value
	e.input.Reset()
	e.updateHeight()
	e.ghostText = ""

	// Slash commands.
	if strings.HasPrefix(fullQuery, "/") {
		return nil, nil, &slashCommandMsg{output: fullQuery}
	}

	// Regular query.
	e.history.Add(fullQuery)
	e.history.Reset()

	return nil, &querySubmitMsg{query: fullQuery}, nil
}

func (e *Editor) handleCancel() (tea.Cmd, *querySubmitMsg, *slashCommandMsg) {
	if e.input.Value() != "" {
		e.input.Reset()
		e.updateHeight()
		e.ghostText = ""
		return nil, nil, nil
	}

	// Empty input + Ctrl+C → no-op hint.
	return nil, nil, nil
}

// View renders the editor with ghost text overlay.
func (e *Editor) View() string {
	v := e.input.View()
	if e.ghostText == "" {
		return v
	}

	// Append dimmed ghost text to the last non-empty line.
	lines := strings.Split(v, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.TrimSpace(lines[i]) != "" {
			ghost := lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(e.ghostText)
			lines[i] += ghost
			break
		}
	}

	return strings.Join(lines, "\n")
}
