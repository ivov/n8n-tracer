package logfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/ivov/n8n-tracer/internal/config"
	"github.com/ivov/n8n-tracer/internal/core"
)

const (
	stateSaveInterval  = 10 * time.Second
	statePruneInterval = 1 * time.Hour
)

type LogfileWatcher struct {
	logDir           string
	stateManager     *StateManager
	watcher          *fsnotify.Watcher
	parser           *core.Parser
	locator          *LogfileLocator
	eventCh          chan interface{}
	errCh            chan error
	closeCh          chan struct{}
	stopOnce         sync.Once
	debounceDuration time.Duration
}

func NewLogfileWatcher(cfg config.Config, sm *StateManager) (*LogfileWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create file watcher: %w", err)
	}

	logDir := filepath.Dir(cfg.LogfileIngestor.WatchFilePath)
	baseFileName := filepath.Base(cfg.LogfileIngestor.WatchFilePath)

	return &LogfileWatcher{
		logDir:           filepath.Dir(cfg.LogfileIngestor.WatchFilePath),
		stateManager:     sm,
		watcher:          watcher,
		parser:           core.NewParser(),
		locator:          NewLogfileLocator(logDir, baseFileName),
		eventCh:          make(chan interface{}),
		errCh:            make(chan error),
		closeCh:          make(chan struct{}),
		debounceDuration: cfg.LogfileIngestor.DebounceDuration,
	}, nil
}

func (watcher *LogfileWatcher) Start(ctx context.Context) (<-chan interface{}, <-chan error) {
	go watcher.run(ctx)

	return watcher.eventCh, watcher.errCh
}

func (watcher *LogfileWatcher) Stop() {
	watcher.stopOnce.Do(func() {
		close(watcher.closeCh)
	})
}

// run is the watcher's main loop of the watcher, continuously scanning
// the target logfiles for changes and processing them into traces
func (watcher *LogfileWatcher) run(ctx context.Context) {
	defer close(watcher.eventCh)
	defer close(watcher.errCh)
	defer watcher.watcher.Close()

	log.Println("Starting initial catch-up scan")
	if err := watcher.scanAndProcess(); err != nil {
		watcher.errCh <- fmt.Errorf("error during initial scan: %w", err)
		return
	}
	log.Println("Completed initial catch-up scan")

	if err := watcher.watcher.Add(watcher.logDir); err != nil {
		watcher.errCh <- fmt.Errorf("failed to watch log directory: %w", err)
		return
	}

	saveTicker := time.NewTicker(stateSaveInterval)
	defer saveTicker.Stop()

	pruneTicker := time.NewTicker(statePruneInterval)
	defer pruneTicker.Stop()

	var debounceTimer *time.Timer
	var debounceChan <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			return

		case <-watcher.closeCh:
			log.Println("Shutting down logfile watcher...")
			if err := watcher.stateManager.Save(); err != nil {
				watcher.errCh <- fmt.Errorf("failed to save state on shutdown: %w", err)
			}
			return

		case event, ok := <-watcher.watcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) {
				if debounceTimer == nil {
					debounceTimer = time.NewTimer(watcher.debounceDuration)
					debounceChan = debounceTimer.C
				} else {
					debounceTimer.Reset(watcher.debounceDuration)
				}
			}

		case <-debounceChan:
			log.Println("Logfile changes, re-scanning and processing")
			if err := watcher.scanAndProcess(); err != nil {
				watcher.errCh <- fmt.Errorf("error during file scan: %w", err)
			}
			debounceTimer.Stop()
			debounceTimer = nil
			debounceChan = nil

		case err, ok := <-watcher.watcher.Errors:
			if !ok {
				return
			}
			watcher.errCh <- fmt.Errorf("watcher error: %w", err)

		case <-saveTicker.C:
			if err := watcher.stateManager.Save(); err != nil {
				watcher.errCh <- fmt.Errorf("failed to periodically save state: %w", err)
			}

		case <-pruneTicker.C:
			log.Println("Pruning old file state...")
			if err := watcher.PruneState(); err != nil {
				watcher.errCh <- fmt.Errorf("failed to prune state: %w", err)
			}
		}
	}
}

func (watcher *LogfileWatcher) scanAndProcess() error {
	logfiles, err := watcher.locator.LocateLogfiles()
	if err != nil {
		return fmt.Errorf("failed to find logfiles: %w", err)
	}

	var totalEventsProcessed int
	for _, filepath := range logfiles {
		processedCount, err := watcher.processLogfile(filepath)
		if err != nil {
			watcher.errCh <- fmt.Errorf("failed to process logfile %s: %w", filepath, err) // log and move on
		}
		totalEventsProcessed += processedCount
	}

	if totalEventsProcessed > 0 {
		log.Printf("Processed %d events", totalEventsProcessed)
	}

	return nil
}

// processLogfile processes a given logfile into events and returns the number
// of events generated out of the logfile
func (watcher *LogfileWatcher) processLogfile(filepath string) (int, error) {
	var eventCount int

	inode, err := GetInode(filepath)
	if err != nil {
		return 0, fmt.Errorf("failed to get inode for %s: %w", filepath, err)
	}

	fileState, _ := watcher.stateManager.Get(inode)
	offset := fileState.Offset

	file, err := os.Open(filepath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek in file: %w", err)
	}

	ioReader := bufio.NewReader(file)
	for {
		line, ioReadErr := ioReader.ReadBytes('\n')

		if ioReadErr == io.EOF {
			break
		}

		if ioReadErr != nil {
			return 0, fmt.Errorf("error reading log line: %w", ioReadErr)
		}

		currentOffset := offset + int64(len(line))

		event, parseErr := watcher.parser.ToEvent(line)
		if parseErr != nil {
			log.Printf("Skipped malformed line at %s: %v", filepath, parseErr)
			watcher.stateManager.Set(inode, currentOffset) // still update offset to skip malformed logline
			offset = currentOffset
			continue
		}

		if event != nil {
			watcher.eventCh <- event
			eventCount++
		}

		watcher.stateManager.Set(inode, currentOffset)
		offset = currentOffset
	}

	return eventCount, nil
}

// pruneState prunes the state of the watcher by clearing from state all logfiles
// that are no longer present in the filesystem
func (watcher *LogfileWatcher) PruneState() error {
	logfiles, err := watcher.locator.LocateLogfiles()
	if err != nil {
		return err
	}

	activeInodes := make(map[uint64]bool)
	for _, filepath := range logfiles {
		inode, err := GetInode(filepath)
		if err != nil {
			log.Printf("failed to get inode for %s during prune: %v", filepath, err)
			continue
		}
		activeInodes[inode] = true
	}

	watcher.stateManager.Prune(activeInodes)
	log.Printf("Pruned state, currently active inodes: %d", len(activeInodes))

	return nil
}
