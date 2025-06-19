package logfile

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"syscall"
)

type State struct {
	Files map[string]FileState `json:"files"` // inode -> FileState
}

type FileState struct {
	Offset int64 `json:"offset"`
}

type StateManager struct {
	stateFilePath string
	state         *State
	mu            sync.Mutex
}

func NewStateManager(path string) (*StateManager, error) {
	sm := &StateManager{
		stateFilePath: path,
		state: &State{
			Files: make(map[string]FileState),
		},
	}

	if err := sm.load(); err != nil {
		return nil, err
	}

	return sm, nil
}

// load reads the state file from disk into memory
func (sm *StateManager) load() error {
	data, err := os.ReadFile(sm.stateFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // This is fine on first run
		}

		return fmt.Errorf("failed to read state file: %w", err)
	}

	return json.Unmarshal(data, sm.state)
}

// Save writes the current state to disk
func (sm *StateManager) Save() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	data, err := json.MarshalIndent(sm.state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Write temp file and use POSIX rename for atomicity
	tempFile := sm.stateFilePath + ".temp"
	if err := os.WriteFile(tempFile, data, 0600); err != nil {
		return fmt.Errorf("failed to write to temp state file: %w", err)
	}

	return os.Rename(tempFile, sm.stateFilePath)
}

// Get returns the processing state for a given inode
func (sm *StateManager) Get(inode uint64) (FileState, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	inodeStr := strconv.FormatUint(inode, 10)
	fileState, found := sm.state.Files[inodeStr]
	return fileState, found
}

// Set updates the processing state for a given inode and offset
func (sm *StateManager) Set(inode uint64, offset int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	inodeStr := strconv.FormatUint(inode, 10)
	sm.state.Files[inodeStr] = FileState{
		Offset: offset,
	}
}

// GetInode retrieves the inode number for a given filepath
func GetInode(filepath string) (uint64, error) {
	info, err := os.Stat(filepath)
	if err != nil {
		return 0, err
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("failed to get syscall.Stat_t for file %s", filepath)
	}

	return stat.Ino, nil
}

// Prune removes state for inodes no longer present on the filesystem
func (sm *StateManager) Prune(activeInodes map[uint64]bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for inodeStr := range sm.state.Files {
		inode, _ := strconv.ParseUint(inodeStr, 10, 64)
		if !activeInodes[inode] {
			delete(sm.state.Files, inodeStr)
		}
	}
}
