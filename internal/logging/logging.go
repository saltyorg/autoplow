package logging

import (
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/saltyorg/autoplow/internal/config"
)

const (
	DefaultLogFilePath = "autoplow.log"
	DefaultMaxSizeMB   = 50
	DefaultMaxBackups  = 5
	DefaultMaxAgeDays  = 30
	DefaultCompress    = true
)

// Apply sets the global log level and output writers (console + rotating file).
// logFilePath is the destination file; when empty, a default filename in the current working directory is used.
func Apply(level string, loader *config.Loader, logFilePath string) {
	applyLevel(level)
	applyOutputs(loader, logFilePath)
}

func applyLevel(level string) {
	switch level {
	case "trace":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}

func applyOutputs(loader *config.Loader, logFilePath string) {
	maxSize := DefaultMaxSizeMB
	maxBackups := DefaultMaxBackups
	maxAgeDays := DefaultMaxAgeDays
	compress := DefaultCompress

	if loader != nil {
		if val := loader.Int("log.max_size_mb", DefaultMaxSizeMB); val > 0 {
			maxSize = val
		}
		if val := loader.Int("log.max_backups", DefaultMaxBackups); val >= 0 {
			maxBackups = val
		}
		if val := loader.Int("log.max_age_days", DefaultMaxAgeDays); val >= 0 {
			maxAgeDays = val
		}
		compress = loader.Bool("log.compress", DefaultCompress)
	}

	if logFilePath == "" {
		logFilePath = DefaultLogFilePath
	}

	consoleOutput := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}
	log.Logger = zerolog.New(consoleOutput).With().Timestamp().Logger()

	if err := ensureLogDir(logFilePath); err != nil {
		log.Error().Err(err).Str("path", logFilePath).Msg("Failed to prepare log directory; logging to console only")
		return
	}

	fileWriter := &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    maxSize,
		MaxBackups: maxBackups,
		MaxAge:     maxAgeDays,
		Compress:   compress,
	}

	fileConsole := zerolog.ConsoleWriter{
		Out:        fileWriter,
		TimeFormat: "2006-01-02 15:04:05",
		NoColor:    true,
	}

	multi := zerolog.MultiLevelWriter(consoleOutput, fileConsole)
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()
}

// FilePathForDB returns a log file path that lives alongside the database file.
func FilePathForDB(dbPath string) string {
	if dbPath == "" {
		return DefaultLogFilePath
	}
	absDBPath, err := filepath.Abs(dbPath)
	if err != nil {
		return filepath.Join(filepath.Dir(dbPath), DefaultLogFilePath)
	}
	return filepath.Join(filepath.Dir(absDBPath), DefaultLogFilePath)
}

func ensureLogDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}
