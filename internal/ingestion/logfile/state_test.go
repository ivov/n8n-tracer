package logfile_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/ivov/n8n-tracer/internal/ingestion/logfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewStateManager_NewFile(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "test-state.json")

	sm, err := logfile.NewStateManager(stateFilePath)

	require.NoError(t, err)
	assert.NotNil(t, sm)

	// Verify empty state on first run
	_, found := sm.Get(12345)
	assert.False(t, found)
}

func Test_NewStateManager_ExistingFile(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "existing-state.json")

	// Create existing state file
	existingState := logfile.State{
		Files: map[string]logfile.FileState{
			"12345": {Offset: 1024},
			"67890": {Offset: 2048},
		},
	}
	data, err := json.Marshal(existingState)
	require.NoError(t, err)

	err = os.WriteFile(stateFilePath, data, 0600)
	require.NoError(t, err)

	sm, err := logfile.NewStateManager(stateFilePath)

	require.NoError(t, err)
	assert.NotNil(t, sm)

	// Verify loaded state
	fileState, found := sm.Get(12345)
	assert.True(t, found)
	assert.Equal(t, int64(1024), fileState.Offset)

	fileState, found = sm.Get(67890)
	assert.True(t, found)
	assert.Equal(t, int64(2048), fileState.Offset)

	_, found = sm.Get(99999)
	assert.False(t, found)
}

func Test_NewStateManager_InvalidJSON(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "invalid-state.json")

	// Create invalid JSON file
	err := os.WriteFile(stateFilePath, []byte("invalid json content"), 0600)
	require.NoError(t, err)

	sm, err := logfile.NewStateManager(stateFilePath)

	require.Error(t, err)
	assert.Nil(t, sm)
	assert.Contains(t, err.Error(), "invalid character")
}

func Test_StateManager_GetAndSet(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "test-state.json")

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	inode := uint64(12345)
	offset := int64(512)

	// Initially not found
	_, found := sm.Get(inode)
	assert.False(t, found)

	// Set and retrieve
	sm.Set(inode, offset)
	fileState, found := sm.Get(inode)
	assert.True(t, found)
	assert.Equal(t, offset, fileState.Offset)

	// Update existing
	newOffset := int64(1024)
	sm.Set(inode, newOffset)
	fileState, found = sm.Get(inode)
	assert.True(t, found)
	assert.Equal(t, newOffset, fileState.Offset)
}

func Test_StateManager_Save(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "save-test.json")

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	// Add some state
	sm.Set(12345, 512)
	sm.Set(67890, 1024)

	// Save to disk
	err = sm.Save()
	require.NoError(t, err)

	// Verify file exists and has correct content
	data, err := os.ReadFile(stateFilePath)
	require.NoError(t, err)

	var savedState logfile.State
	err = json.Unmarshal(data, &savedState)
	require.NoError(t, err)

	assert.Len(t, savedState.Files, 2)
	assert.Equal(t, int64(512), savedState.Files["12345"].Offset)
	assert.Equal(t, int64(1024), savedState.Files["67890"].Offset)
}

func Test_StateManager_Save_AtomicWrite(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "atomic-test.json")

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	sm.Set(12345, 512)

	err = sm.Save()
	require.NoError(t, err)

	// Verify temp file was cleaned up
	tempFile := stateFilePath + ".temp"
	_, err = os.Stat(tempFile)
	assert.True(t, os.IsNotExist(err), "Temp file should be cleaned up after save")

	// Verify main file exists
	_, err = os.Stat(stateFilePath)
	require.NoError(t, err)
}

func Test_StateManager_Save_ReadOnlyDirectory(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "readonly-test.json")

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	sm.Set(12345, 512)

	// Make directory read-only
	err = os.Chmod(tempDir, 0444)
	require.NoError(t, err)

	// Restore permissions in cleanup
	defer func() {
		err := os.Chmod(tempDir, 0755)
		require.NoError(t, err)
	}()

	err = sm.Save()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write to temp state file")
}

func Test_StateManager_Prune(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "prune-test.json")

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	// Add multiple entries
	sm.Set(12345, 512)
	sm.Set(67890, 1024)
	sm.Set(11111, 2048)
	sm.Set(22222, 4096)

	// Define active inodes (subset of what we have)
	activeInodes := map[uint64]bool{
		12345: true,
		11111: true,
		// 67890 and 22222 are not active and should be pruned
	}

	sm.Prune(activeInodes)

	// Verify only active inodes remain
	_, found := sm.Get(12345)
	assert.True(t, found, "Active inode should remain")

	_, found = sm.Get(11111)
	assert.True(t, found, "Active inode should remain")

	_, found = sm.Get(67890)
	assert.False(t, found, "Inactive inode should be pruned")

	_, found = sm.Get(22222)
	assert.False(t, found, "Inactive inode should be pruned")
}

func Test_StateManager_Prune_EmptyActiveInodes(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "prune-empty-test.json")

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	// Add entries
	sm.Set(12345, 512)
	sm.Set(67890, 1024)

	// Prune with empty active inodes
	activeInodes := map[uint64]bool{}
	sm.Prune(activeInodes)

	// All should be pruned
	_, found := sm.Get(12345)
	assert.False(t, found)

	_, found = sm.Get(67890)
	assert.False(t, found)
}

func Test_StateManager_Prune_NoChanges(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "prune-nochange-test.json")

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	// Add entries
	sm.Set(12345, 512)
	sm.Set(67890, 1024)

	// All inodes are active
	activeInodes := map[uint64]bool{
		12345: true,
		67890: true,
	}

	sm.Prune(activeInodes)

	// All should remain
	_, found := sm.Get(12345)
	assert.True(t, found)

	_, found = sm.Get(67890)
	assert.True(t, found)
}

func Test_GetInode_ValidFile(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test-file.txt")

	err := os.WriteFile(testFile, []byte("test content"), 0600)
	require.NoError(t, err)

	inode, err := logfile.GetInode(testFile)

	require.NoError(t, err)
	assert.Greater(t, inode, uint64(0), "Inode should be a positive number")
}

func Test_GetInode_NonexistentFile(t *testing.T) {
	inode, err := logfile.GetInode("/nonexistent/file.txt")

	require.Error(t, err)
	assert.Equal(t, uint64(0), inode)
	assert.True(t, os.IsNotExist(err))
}

func Test_GetInode_Directory(t *testing.T) {
	tempDir := t.TempDir()

	inode, err := logfile.GetInode(tempDir)

	require.NoError(t, err)
	assert.Greater(t, inode, uint64(0), "Directory should have a valid inode")
}

func Test_GetInode_ConsistentResults(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "consistent-test.txt")

	err := os.WriteFile(testFile, []byte("test content"), 0600)
	require.NoError(t, err)

	inode1, err := logfile.GetInode(testFile)
	require.NoError(t, err)

	inode2, err := logfile.GetInode(testFile)
	require.NoError(t, err)

	assert.Equal(t, inode1, inode2, "Multiple calls should return same inode")
}

func Test_StateManager_RoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "roundtrip-test.json")

	// Create first state manager and add data
	sm1, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	sm1.Set(12345, 512)
	sm1.Set(67890, 1024)
	sm1.Set(11111, 2048)

	err = sm1.Save()
	require.NoError(t, err)

	// Create second state manager and verify it loads same data
	sm2, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	fileState, found := sm2.Get(12345)
	assert.True(t, found)
	assert.Equal(t, int64(512), fileState.Offset)

	fileState, found = sm2.Get(67890)
	assert.True(t, found)
	assert.Equal(t, int64(1024), fileState.Offset)

	fileState, found = sm2.Get(11111)
	assert.True(t, found)
	assert.Equal(t, int64(2048), fileState.Offset)

	_, found = sm2.Get(99999)
	assert.False(t, found)
}

func Test_StateManager_ZeroOffset(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "zero-offset-test.json")

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	// Set offset to zero (valid case)
	sm.Set(12345, 0)

	fileState, found := sm.Get(12345)
	assert.True(t, found)
	assert.Equal(t, int64(0), fileState.Offset)

	// Save and reload to ensure zero persists
	err = sm.Save()
	require.NoError(t, err)

	sm2, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	fileState, found = sm2.Get(12345)
	assert.True(t, found)
	assert.Equal(t, int64(0), fileState.Offset)
}

func Test_StateManager_LargeOffsets(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "large-offset-test.json")

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	// Test with large offset values
	largeOffset := int64(9223372036854775807) // max int64
	sm.Set(12345, largeOffset)

	fileState, found := sm.Get(12345)
	assert.True(t, found)
	assert.Equal(t, largeOffset, fileState.Offset)

	// Save and reload
	err = sm.Save()
	require.NoError(t, err)

	sm2, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	fileState, found = sm2.Get(12345)
	assert.True(t, found)
	assert.Equal(t, largeOffset, fileState.Offset)
}

func Test_StateManager_MultipleInodes(t *testing.T) {
	tempDir := t.TempDir()
	stateFilePath := filepath.Join(tempDir, "multi-inode-test.json")

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	// Test with multiple different inode values
	testData := map[uint64]int64{
		1:                    100,
		12345:                512,
		67890:                1024,
		999999999:            2048,
		18446744073709551615: 4096, // max uint64
	}

	for inode, offset := range testData {
		sm.Set(inode, offset)
	}

	for inode, expectedOffset := range testData {
		fileState, found := sm.Get(inode)
		assert.True(t, found, "Inode %d should be found", inode)
		assert.Equal(t, expectedOffset, fileState.Offset, "Offset for inode %d should match", inode)
	}

	// Save and reload to test persistence
	err = sm.Save()
	require.NoError(t, err)

	sm2, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	// Verify persistence
	for inode, expectedOffset := range testData {
		fileState, found := sm2.Get(inode)
		assert.True(t, found, "Inode %d should be found after reload", inode)
		assert.Equal(t, expectedOffset, fileState.Offset, "Offset for inode %d should match after reload", inode)
	}
}

func Test_GetInode_RealFile(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "real-file.txt")

	content := []byte("This is test content for inode testing")
	err := os.WriteFile(testFile, content, 0600)
	require.NoError(t, err)

	inode, err := logfile.GetInode(testFile)
	require.NoError(t, err)

	// Verify the inode is what we expect by checking file info
	fileInfo, err := os.Stat(testFile)
	require.NoError(t, err)

	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	require.True(t, ok, "Should be able to get syscall.Stat_t")

	assert.Equal(t, stat.Ino, inode, "Inode from GetInode should match syscall stat")
}
