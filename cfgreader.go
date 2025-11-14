package cfgreader

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ConfigFormat represents supported configuration file formats
type ConfigFormat int

const (
	FormatUnknown ConfigFormat = iota
	FormatYAML
	FormatJSON

	DefaultFormatsCount
)

type UnmarshalFunc func(data []byte, v any) error

type FormatData struct {
	Name       string
	Extensions []string
	Unmarshal  UnmarshalFunc
}

type FormatMap map[ConfigFormat]FormatData

var DefaultFormats = FormatMap{
	FormatUnknown: {Name: "unknown", Extensions: []string{""}},
	FormatYAML:    {Name: "yaml", Extensions: []string{".yaml", ".yml"}, Unmarshal: yaml.Unmarshal},
	FormatJSON:    {Name: "json", Extensions: []string{".json"}, Unmarshal: json.Unmarshal},
}

// ConfigReader handles reading and parsing configuration files with generics
type ConfigReader[T any] struct {
	logger        *slog.Logger
	defaultPath   string
	supportedExts map[string]ConfigFormat
	formats       FormatMap
	strictMode    bool
	maxFileSize   int64
	recursive     bool
}

// ConfigReaderOption provides functional options for ConfigReader
type ConfigReaderOption[T any] func(*ConfigReader[T])

// WithLogger sets a custom logger
func WithLogger[T any](logger *slog.Logger) ConfigReaderOption[T] {
	return func(cr *ConfigReader[T]) {
		cr.logger = logger
	}
}

// WithStrictMode enables strict parsing (fails on any error)
func WithStrictMode[T any](strict bool) ConfigReaderOption[T] {
	return func(cr *ConfigReader[T]) {
		cr.strictMode = strict
	}
}

// WithMaxFileSize sets maximum allowed file size in bytes
func WithMaxFileSize[T any](size int64) ConfigReaderOption[T] {
	return func(cr *ConfigReader[T]) {
		cr.maxFileSize = size
	}
}

// WithDefaultPath sets the default configuration path (file or directory)
func WithDefaultPath[T any](path string) ConfigReaderOption[T] {
	return func(cr *ConfigReader[T]) {
		cr.defaultPath = path
	}
}

// WithRecursive enables recursive directory scanning
func WithRecursive[T any](recursive bool) ConfigReaderOption[T] {
	return func(cr *ConfigReader[T]) {
		cr.recursive = recursive
	}
}

// NewConfigReader creates a new ConfigReader with sensible defaults
func NewConfigReader[T any](opts ...ConfigReaderOption[T]) *ConfigReader[T] {
	cr := &ConfigReader[T]{
		logger:        slog.Default(),
		defaultPath:   "/etc/config",
		maxFileSize:   10 * 1024 * 1024, // 10MB default
		strictMode:    false,
		recursive:     false,
		supportedExts: make(map[string]ConfigFormat),
		formats:       make(FormatMap),
	}
	cr.RegisterFormats(DefaultFormats)

	// Apply options
	for _, opt := range opts {
		opt(cr)
	}

	return cr
}

// RegisterFormat allows users to add custom format support
func (cr *ConfigReader[T]) RegisterFormats(formats FormatMap) {
	for formatID, formatData := range formats {
		for _, ext := range formatData.Extensions {
			cr.supportedExts[strings.ToLower(ext)] = formatID
		}
	}
	maps.Copy(cr.formats, formats)
}

// detectFormat identifies the configuration format from file extension
func (cr *ConfigReader[T]) detectFormat(filename string) ConfigFormat {
	ext := strings.ToLower(filepath.Ext(filename))
	if format, exists := cr.supportedExts[ext]; exists {
		return format
	}
	return FormatUnknown
}

// readAndParseFile reads a file and unmarshals it into the target structure
func (cr *ConfigReader[T]) readAndParseFile(fullPath string, format ConfigFormat, target *T) error {
	cr.logger.Debug("reading configuration file",
		slog.String("path", fullPath),
		slog.String("format", cr.formats[format].Name))

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	cr.logger.Debug("file read successfully",
		slog.String("path", fullPath),
		slog.Int("bytes", len(data)))

	formatItem, exists := cr.formats[format]
	if !exists {
		return fmt.Errorf("no registered format: %s", formatItem.Name)
	}

	if err := formatItem.Unmarshal(data, target); err != nil {
		return fmt.Errorf("failed to unmarshal %s: %w", formatItem.Name, err)
	}

	cr.logger.Debug("file parsed successfully",
		slog.String("path", fullPath),
		slog.String("format", formatItem.Name))

	return nil
}

// processFile handles the complete lifecycle of reading and parsing a single file
func (cr *ConfigReader[T]) processFile(fullPath string, info fs.FileInfo) (*T, string, error) {
	filename := info.Name()

	if info.IsDir() {
		return nil, baseName(filename), fmt.Errorf("path is a directory")
	}

	if info.Size() > cr.maxFileSize {
		return nil, baseName(filename), fmt.Errorf("file size %d exceeds maximum allowed size %d", info.Size(), cr.maxFileSize)
	}

	format := cr.detectFormat(info.Name())
	if format == FormatUnknown {
		return nil, baseName(filename), fmt.Errorf("unsupported file format")
	}

	var cfg T
	if err := cr.readAndParseFile(fullPath, format, &cfg); err != nil {
		return nil, "", err
	}

	return &cfg, baseName(filename), nil
}

// ReadFile reads and parses a single configuration file
func (cr *ConfigReader[T]) ReadFile(filePath string) (*T, error) {
	if filePath == "" {
		filePath = cr.defaultPath
		cr.logger.Info("using default configuration file",
			slog.String("path", filePath))
	}

	cr.logger.Info("reading configuration file",
		slog.String("path", filePath))

	info, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("configuration file inaccessible: %w", err)
	}

	if info.IsDir() {
		return nil, fmt.Errorf("path is a directory, use ReadDir or ReadDirMap instead")
	}

	cfg, _, err := cr.processFile(filePath, info)
	if err != nil {
		return nil, fmt.Errorf("failed to process file: %w", err)
	}

	cr.logger.Info("configuration loaded successfully",
		slog.String("path", filePath))

	return cfg, nil
}

// ReadDir reads all configuration files from a directory and returns a slice
func (cr *ConfigReader[T]) ReadDir(dirPath string) ([]*T, error) {
	configs, err := cr.ReadDirMap(dirPath)
	if err != nil {
		return nil, err
	}

	result := make([]*T, 0, len(configs))
	for _, cfg := range configs {
		result = append(result, cfg)
	}

	return result, nil
}

// ReadDirMap reads all configuration files from a directory and returns a map
// Key is the service name (filename without extension)
func (cr *ConfigReader[T]) ReadDirMap(dirPath string) (map[string]*T, error) {
	if dirPath == "" {
		dirPath = cr.defaultPath
		cr.logger.Info("using default configuration directory",
			slog.String("dir", dirPath))
	}

	cr.logger.Info("scanning configuration directory",
		slog.String("dir", dirPath),
		slog.Bool("strict_mode", cr.strictMode),
		slog.Bool("recursive", cr.recursive))

	// Verify directory exists and is accessible
	info, err := os.Stat(dirPath)
	if err != nil {
		return nil, fmt.Errorf("configuration directory inaccessible: %w", err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("path is not a directory, use ReadFile instead")
	}

	configs := make(map[string]*T)
	stats := &scanStats{}

	if cr.recursive {
		err = cr.scanRecursive(dirPath, configs, stats)
	} else {
		err = cr.scanFlat(dirPath, configs, stats)
	}

	if err != nil {
		return nil, err
	}

	cr.logger.Info("configuration loading complete",
		slog.Int("processed", stats.processed),
		slog.Int("skipped", stats.skipped),
		slog.Int("errors", stats.errors),
		slog.Int("total_services", len(configs)))

	if len(configs) == 0 {
		cr.logger.Warn("no valid configuration files found",
			slog.String("dir", dirPath))
	}

	return configs, nil
}

// scanStats tracks statistics during directory scanning
type scanStats struct {
	processed int
	skipped   int
	errors    int
}

// scanFlat scans a single directory level (non-recursive)
func (cr *ConfigReader[T]) scanFlat(dirPath string, configs map[string]*T, stats *scanStats) error {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	cr.logger.Info("directory scan complete",
		slog.String("dir", dirPath),
		slog.Int("total_entries", len(files)))

	for _, file := range files {
		if file.IsDir() {
			stats.skipped++
			continue
		}

		if err := cr.processEntry(dirPath, file, configs, stats); err != nil {
			if cr.strictMode {
				return err
			}
		}
	}

	return nil
}

// scanRecursive scans directories recursively
func (cr *ConfigReader[T]) scanRecursive(dirPath string, configs map[string]*T, stats *scanStats) error {
	return filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			cr.logger.Warn("error accessing path during recursive scan",
				slog.String("path", path),
				slog.String("error", err.Error()))
			if cr.strictMode {
				return fmt.Errorf("strict mode: failed to access %s: %w", path, err)
			}
			stats.errors++
			return nil // Continue walking
		}

		if d.IsDir() {
			return nil // Continue into subdirectories
		}

		parentDir := filepath.Dir(path)
		return cr.processEntry(parentDir, d, configs, stats)
	})
}

// processEntry handles a single directory entry
func (cr *ConfigReader[T]) processEntry(parentDir string, entry fs.DirEntry, configs map[string]*T, stats *scanStats) error {
	filename := entry.Name()
	fullPath := filepath.Join(parentDir, filename)

	info, err := entry.Info()
	if err != nil {
		stats.errors++
		cr.logger.Warn("failed to get file info",
			slog.String("file", filename),
			slog.String("error", err.Error()))
		if cr.strictMode {
			return fmt.Errorf("strict mode: failed to get info for %s: %w", filename, err)
		}
		stats.skipped++
		return nil
	}

	cfg, baseName, err := cr.processFile(fullPath, info)
	if err != nil {
		stats.errors++
		cr.logger.Warn("failed to process configuration file",
			slog.String("file", fullPath),
			slog.String("error", err.Error()))

		if cr.strictMode {
			return fmt.Errorf("strict mode: failed to process %s: %w", filename, err)
		}
		stats.skipped++
		return nil
	}

	if cfg == nil {
		stats.skipped++
		return nil
	}

	// Check for duplicate service names
	if _, duplicate := configs[baseName]; duplicate {
		cr.logger.Warn("duplicate service name detected",
			slog.String("service", baseName),
			slog.String("file", fullPath))

		if cr.strictMode {
			return fmt.Errorf("strict mode: duplicate service name '%s' in file %s", baseName, filename)
		}
		cr.logger.Info("overwriting previous configuration",
			slog.String("service", baseName))
	}

	configs[baseName] = cfg
	stats.processed++

	cr.logger.Info("configuration loaded successfully",
		slog.String("service", baseName),
		slog.String("file", fullPath))

	return nil
}

// Read is a smart method that automatically detects if the path is a file or directory
func (cr *ConfigReader[T]) Read(path string) (content any, isDir bool, err error) {
	if path == "" {
		path = cr.defaultPath
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, false, fmt.Errorf("path inaccessible: %w", err)
	}

	if info.IsDir() {
		cr.logger.Info("detected directory, using ReadDirMap",
			slog.String("path", path))
		content, err = cr.ReadDirMap(path)
		return content, true, err
	}

	cr.logger.Info("detected file, using ReadFile",
		slog.String("path", path))
	content, err = cr.ReadFile(path)
	return content, false, err
}
