package meta

import (
	"io"
	"log"
	"log/slog"

	"github.com/hashicorp/go-hclog"
)

// hclogAdapter bridges hashicorp/go-hclog to Go's slog.Logger.
// hashicorp/raft requires an hclog.Logger; this adapter forwards
// all log calls to our slog.Logger.
type hclogAdapter struct {
	logger *slog.Logger
	name   string
	args   []interface{}
}

func newHCLogAdapter(logger *slog.Logger) hclog.Logger {
	return &hclogAdapter{logger: logger}
}

func (h *hclogAdapter) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Trace, hclog.Debug:
		h.logger.Debug(msg, args...)
	case hclog.Info:
		h.logger.Info(msg, args...)
	case hclog.Warn:
		h.logger.Warn(msg, args...)
	case hclog.Error:
		h.logger.Error(msg, args...)
	}
}

func (h *hclogAdapter) Trace(msg string, args ...interface{}) {
	h.logger.Debug(msg, args...)
}

func (h *hclogAdapter) Debug(msg string, args ...interface{}) {
	h.logger.Debug(msg, args...)
}

func (h *hclogAdapter) Info(msg string, args ...interface{}) {
	h.logger.Info(msg, args...)
}

func (h *hclogAdapter) Warn(msg string, args ...interface{}) {
	h.logger.Warn(msg, args...)
}

func (h *hclogAdapter) Error(msg string, args ...interface{}) {
	h.logger.Error(msg, args...)
}

func (h *hclogAdapter) IsTrace() bool { return false }
func (h *hclogAdapter) IsDebug() bool { return true }
func (h *hclogAdapter) IsInfo() bool  { return true }
func (h *hclogAdapter) IsWarn() bool  { return true }
func (h *hclogAdapter) IsError() bool { return true }

func (h *hclogAdapter) ImpliedArgs() []interface{} { return h.args }

func (h *hclogAdapter) With(args ...interface{}) hclog.Logger {
	return &hclogAdapter{
		logger: h.logger.With(args...),
		name:   h.name,
		args:   append(h.args, args...),
	}
}

func (h *hclogAdapter) Name() string { return h.name }

func (h *hclogAdapter) Named(name string) hclog.Logger {
	newName := name
	if h.name != "" {
		newName = h.name + "." + name
	}

	return &hclogAdapter{
		logger: h.logger.With("component", newName),
		name:   newName,
		args:   h.args,
	}
}

func (h *hclogAdapter) ResetNamed(name string) hclog.Logger {
	return &hclogAdapter{
		logger: h.logger.With("component", name),
		name:   name,
		args:   h.args,
	}
}

func (h *hclogAdapter) SetLevel(hclog.Level) {}

func (h *hclogAdapter) GetLevel() hclog.Level { return hclog.Info }

func (h *hclogAdapter) StandardLogger(_ *hclog.StandardLoggerOptions) *log.Logger {
	return log.Default()
}

func (h *hclogAdapter) StandardWriter(_ *hclog.StandardLoggerOptions) io.Writer {
	return io.Discard
}
