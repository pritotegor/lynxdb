package shell

import "charm.land/bubbles/v2/key"

type keyMap struct {
	Submit        key.Binding
	Quit          key.Binding
	Cancel        key.Binding
	ClearScr      key.Binding
	HistPrev      key.Binding
	HistNext      key.Binding
	AcceptSugg    key.Binding
	InsertNewline key.Binding
	ScrollUp      key.Binding
	ScrollDn      key.Binding
	FocusBack     key.Binding
}

func defaultKeyMap() keyMap {
	return keyMap{
		Submit: key.NewBinding(
			key.WithKeys("enter"),
		),
		Quit: key.NewBinding(
			key.WithKeys("ctrl+d"),
		),
		Cancel: key.NewBinding(
			key.WithKeys("ctrl+c"),
		),
		ClearScr: key.NewBinding(
			key.WithKeys("ctrl+l"),
		),
		HistPrev: key.NewBinding(
			key.WithKeys("ctrl+p"),
		),
		HistNext: key.NewBinding(
			key.WithKeys("ctrl+n"),
		),
		AcceptSugg: key.NewBinding(
			key.WithKeys("tab"),
		),
		InsertNewline: key.NewBinding(
			key.WithKeys("shift+enter"),
		),
		ScrollUp: key.NewBinding(
			key.WithKeys("pgup"),
		),
		ScrollDn: key.NewBinding(
			key.WithKeys("pgdown"),
		),
		FocusBack: key.NewBinding(
			key.WithKeys("esc"),
		),
	}
}
