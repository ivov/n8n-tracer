package logfile_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ivov/n8n-tracer/internal/ingestion/logfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewLogfileLocator(t *testing.T) {
	logDir := "/test/dir"
	baseFileName := "n8nEventLog.log"

	locator := logfile.NewLogfileLocator(logDir, baseFileName)

	assert.NotNil(t, locator)
}

func Test_LocateLogfiles_EmptyDirectory(t *testing.T) {
	tempDir := t.TempDir()
	locator := logfile.NewLogfileLocator(tempDir, "n8nEventLog.log")

	logfiles, err := locator.LocateLogfiles()

	require.NoError(t, err)
	assert.Empty(t, logfiles)
}

func Test_LocateLogfiles_SingleLogfile(t *testing.T) {
	tempDir := t.TempDir()
	logfilePath := filepath.Join(tempDir, "n8nEventLog.log")

	err := os.WriteFile(logfilePath, []byte("test content"), 0600)
	require.NoError(t, err)

	locator := logfile.NewLogfileLocator(tempDir, "n8nEventLog.log")

	logfiles, err := locator.LocateLogfiles()

	require.NoError(t, err)
	require.Len(t, logfiles, 1)
	assert.Equal(t, logfilePath, logfiles[0])
}

func Test_LocateLogfiles_MultipleRotatedLogfiles(t *testing.T) {
	tempDir := t.TempDir()

	filenames := []string{
		"n8nEventLog.log",    // newest (no suffix)
		"n8nEventLog-1.log",  // second newest
		"n8nEventLog-2.log",  // third newest
		"n8nEventLog-10.log", // oldest (double digit)
	}

	for _, filename := range filenames {
		err := os.WriteFile(filepath.Join(tempDir, filename), []byte("test"), 0600)
		require.NoError(t, err)
	}

	locator := logfile.NewLogfileLocator(tempDir, "n8nEventLog.log")

	logfiles, err := locator.LocateLogfiles()

	require.NoError(t, err)
	require.Len(t, logfiles, 4)

	// Verify chronological order (oldest first)
	expectedOrder := []string{
		filepath.Join(tempDir, "n8nEventLog-10.log"), // oldest
		filepath.Join(tempDir, "n8nEventLog-2.log"),
		filepath.Join(tempDir, "n8nEventLog-1.log"),
		filepath.Join(tempDir, "n8nEventLog.log"), // newest
	}

	assert.Equal(t, expectedOrder, logfiles)
}

func Test_LocateLogfiles_MixedFiles(t *testing.T) {
	tempDir := t.TempDir()

	files := []string{
		"n8nEventLog.log",
		"n8nEventLog-1.log",
		"n8nEventLog-2.log",
		"other.log",             // different base name
		"n8nEventLog.txt",       // different extension
		"n8nEventLog-abc.log",   // non-numeric suffix
		"n8nEventLog-1.log.bak", // backup file
		"somefile.txt",          // completely different
	}

	for _, filename := range files {
		err := os.WriteFile(filepath.Join(tempDir, filename), []byte("test"), 0600)
		require.NoError(t, err)
	}

	locator := logfile.NewLogfileLocator(tempDir, "n8nEventLog.log")

	logfiles, err := locator.LocateLogfiles()

	require.NoError(t, err)
	require.Len(t, logfiles, 3, "Should only match n8nEventLog.log and rotated versions")

	// Verify only correct files are included
	expectedFiles := []string{
		filepath.Join(tempDir, "n8nEventLog-2.log"),
		filepath.Join(tempDir, "n8nEventLog-1.log"),
		filepath.Join(tempDir, "n8nEventLog.log"),
	}

	assert.Equal(t, expectedFiles, logfiles)
}

func Test_LocateLogfiles_WithSubdirectories(t *testing.T) {
	tempDir := t.TempDir()

	// Create log files and subdirectories
	err := os.WriteFile(filepath.Join(tempDir, "n8nEventLog.log"), []byte("test"), 0600)
	require.NoError(t, err)

	err = os.WriteFile(filepath.Join(tempDir, "n8nEventLog-1.log"), []byte("test"), 0600)
	require.NoError(t, err)

	// Create subdirectory with same-named file (should be ignored)
	subDir := filepath.Join(tempDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	require.NoError(t, err)

	err = os.WriteFile(filepath.Join(subDir, "n8nEventLog.log"), []byte("test"), 0600)
	require.NoError(t, err)

	locator := logfile.NewLogfileLocator(tempDir, "n8nEventLog.log")

	logfiles, err := locator.LocateLogfiles()

	require.NoError(t, err)
	require.Len(t, logfiles, 2, "Should only find files in target directory, not subdirectories")

	expectedFiles := []string{
		filepath.Join(tempDir, "n8nEventLog-1.log"),
		filepath.Join(tempDir, "n8nEventLog.log"),
	}

	assert.Equal(t, expectedFiles, logfiles)
}

func Test_LocateLogfiles_NonexistentDirectory(t *testing.T) {
	locator := logfile.NewLogfileLocator("/nonexistent/directory", "n8nEventLog.log")

	logfiles, err := locator.LocateLogfiles()

	require.Error(t, err)
	assert.Nil(t, logfiles)
	assert.Contains(t, err.Error(), "failed to read dir")
}

func Test_LocateLogfiles_DifferentBaseFileName(t *testing.T) {
	tempDir := t.TempDir()

	// Create files with different base names
	files := []string{
		"mylog.log",
		"mylog-1.log",
		"mylog-2.log",
		"otherlog.log",
		"otherlog-1.log",
	}

	for _, filename := range files {
		err := os.WriteFile(filepath.Join(tempDir, filename), []byte("test"), 0600)
		require.NoError(t, err)
	}

	locator := logfile.NewLogfileLocator(tempDir, "mylog.log")

	logfiles, err := locator.LocateLogfiles()

	require.NoError(t, err)
	require.Len(t, logfiles, 3)

	expectedFiles := []string{
		filepath.Join(tempDir, "mylog-2.log"),
		filepath.Join(tempDir, "mylog-1.log"),
		filepath.Join(tempDir, "mylog.log"),
	}

	assert.Equal(t, expectedFiles, logfiles)
}

func Test_LocateLogfiles_SpecialCharactersInBaseName(t *testing.T) {
	tempDir := t.TempDir()

	baseFileName := "my-special.log.file.log"
	files := []string{
		"my-special.log.file.log",
		"my-special.log.file-1.log",
		"my-special.log.file-2.log",
		"my-special.log.file.txt", // different extension, should not match
	}

	for _, filename := range files {
		err := os.WriteFile(filepath.Join(tempDir, filename), []byte("test"), 0600)
		require.NoError(t, err)
	}

	locator := logfile.NewLogfileLocator(tempDir, baseFileName)

	logfiles, err := locator.LocateLogfiles()

	require.NoError(t, err)
	require.Len(t, logfiles, 3)

	expectedFiles := []string{
		filepath.Join(tempDir, "my-special.log.file-2.log"),
		filepath.Join(tempDir, "my-special.log.file-1.log"),
		filepath.Join(tempDir, "my-special.log.file.log"),
	}

	assert.Equal(t, expectedFiles, logfiles)
}

func Test_LocateLogfiles_LargeNumberedSuffix(t *testing.T) {
	tempDir := t.TempDir()

	// Create files with large numbered suffixes
	files := []string{
		"n8nEventLog.log",
		"n8nEventLog-1.log",
		"n8nEventLog-100.log",
		"n8nEventLog-999.log",
	}

	for _, filename := range files {
		err := os.WriteFile(filepath.Join(tempDir, filename), []byte("test"), 0600)
		require.NoError(t, err)
	}

	locator := logfile.NewLogfileLocator(tempDir, "n8nEventLog.log")

	logfiles, err := locator.LocateLogfiles()

	require.NoError(t, err)
	require.Len(t, logfiles, 4)

	// Verify chronological order (oldest first)
	expectedFiles := []string{
		filepath.Join(tempDir, "n8nEventLog-999.log"),
		filepath.Join(tempDir, "n8nEventLog-100.log"),
		filepath.Join(tempDir, "n8nEventLog-1.log"),
		filepath.Join(tempDir, "n8nEventLog.log"), // newest (no suffix)
	}

	assert.Equal(t, expectedFiles, logfiles)
}

func Test_LocateLogfiles_OnlyRotatedFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Create only rotated files, no base file (should find them all anyway)
	files := []string{
		"n8nEventLog-1.log",
		"n8nEventLog-2.log",
		"n8nEventLog-3.log",
	}

	for _, filename := range files {
		err := os.WriteFile(filepath.Join(tempDir, filename), []byte("test"), 0600)
		require.NoError(t, err)
	}

	locator := logfile.NewLogfileLocator(tempDir, "n8nEventLog.log")

	logfiles, err := locator.LocateLogfiles()

	require.NoError(t, err)
	require.Len(t, logfiles, 3)

	expectedFiles := []string{
		filepath.Join(tempDir, "n8nEventLog-3.log"),
		filepath.Join(tempDir, "n8nEventLog-2.log"),
		filepath.Join(tempDir, "n8nEventLog-1.log"),
	}

	assert.Equal(t, expectedFiles, logfiles)
}
