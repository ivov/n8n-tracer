package logfile

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type LogfileLocator struct {
	logDir       string
	baseFileName string
}

func NewLogfileLocator(logDir, baseFileName string) *LogfileLocator {
	return &LogfileLocator{
		logDir:       logDir,
		baseFileName: baseFileName,
	}
}

// Return all log files in the target dir, in chronological order
func (locator *LogfileLocator) LocateLogfiles() ([]string, error) {
	dir := locator.logDir
	baseName := locator.baseFileName

	// e.g. match n8nEventLog.log and n8nEventLog-{n}.log
	pattern := regexp.MustCompile(`^` + regexp.QuoteMeta(strings.TrimSuffix(baseName, ".log")) + `(-\d+)?\.log$`)

	fsEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read dir %s: %w", dir, err)
	}

	var logfiles []string
	for _, fsEntry := range fsEntries {
		if fsEntry.IsDir() {
			continue
		}
		if pattern.MatchString(fsEntry.Name()) {
			logfiles = append(logfiles, filepath.Join(dir, fsEntry.Name()))
		}
	}

	// Sort by oldest first to ensure chronological order
	sort.Slice(logfiles, func(i, j int) bool {
		numI := locator.logfileNumber(logfiles[i])
		numJ := locator.logfileNumber(logfiles[j])
		return numI > numJ
	})

	return logfiles, nil
}

// Extract the sequence number from a logfile name, e.g. `n8nEventLog-1.log` -> `1`
func (locator *LogfileLocator) logfileNumber(filename string) int {
	base := filepath.Base(filename)
	if strings.HasSuffix(base, ".log") && !strings.Contains(base, "-") {
		return -1 // n8nEventLog.log (newest, unsuffixed)
	}

	parts := strings.Split(base, "-")
	if len(parts) < 2 {
		return -1
	}

	numStr := strings.TrimSuffix(parts[len(parts)-1], ".log")
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return -1
	}

	return num
}
