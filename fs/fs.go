package fs

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/cyverse-de/model"
	"github.com/pkg/errors"
)

// adapted from https://talks.golang.org/2012/10things.slide#8

// FS is a FileSystem that interacts with the local filesystem.
var FS FileSystem = localFS{}

// FileSystem defines the filesystem operations used in this file of road-runner
type FileSystem interface {
	Open(path string) (File, error)
	Create(path string) (File, error)
	Remove(path string) error
}

// File defines the operations supported by File objects.
type File interface {
	io.Closer
	io.Reader
	io.Writer
}

// localFS implements FileSystem using the local filesystem.
type localFS struct{}

func (localFS) Open(path string) (File, error)   { return os.Open(path) }
func (localFS) Create(path string) (File, error) { return os.Create(path) }
func (localFS) Remove(path string) error         { return os.Remove(path) }

// CopyJobFile copies the contents of from to a file called <uuid>.json inside
// the directory specified by toDir.
func CopyJobFile(fs FileSystem, uuid, from, toDir string) error {
	inputReader, err := fs.Open(from)
	if err != nil {
		return errors.Wrapf(err, "failed to open %s", from)
	}
	defer inputReader.Close()
	outputFilePath := path.Join(toDir, fmt.Sprintf("%s.json", uuid))
	outputWriter, err := fs.Create(outputFilePath)
	if err != nil {
		return errors.Wrapf(err, "failed to write to %s", outputFilePath)
	}
	defer outputWriter.Close()
	if _, err := io.Copy(outputWriter, inputReader); err != nil {
		return errors.Wrapf(err, "failed to copy contents of %s to %s", from, toDir)
	}
	return nil
}

// WriteJob file write the contents of the file passed in to the file called
// <uuid>.json inside the directory specified by toDir.
func WriteJob(fs FileSystem, uuid, toDir string, content []byte) error {
	outputFilePath := path.Join(toDir, fmt.Sprintf("%s.json", uuid))

	outputWriter, err := fs.Create(outputFilePath)
	if err != nil {
		return errors.Wrapf(err, "failed to create path %s", outputFilePath)
	}
	defer outputWriter.Close()

	buf := bytes.NewBuffer(content)
	if _, err := io.Copy(outputWriter, buf); err != nil {
		return errors.Wrapf(err, "failed to write content to %s", outputFilePath)
	}
	return nil
}

// CopyFile will copy a file from 'from' to 'to'.
func CopyFile(fs FileSystem, from, to string) error {
	inputReader, err := fs.Open(from)
	if err != nil {
		return errors.Wrapf(err, "failed to open %s", from)
	}
	defer inputReader.Close()

	outputWriter, err := fs.Create(to)
	if err != nil {
		return errors.Wrapf(err, "failed to write to %s", to)
	}
	defer outputWriter.Close()

	if _, err := io.Copy(outputWriter, inputReader); err != nil {
		return errors.Wrapf(err, "failed to copy contents of %s to %s", from, to)
	}

	return nil
}

// DeleteJobFile deletes the file <uuid>.json from the directory specified by
// toDir.
func DeleteJobFile(fs FileSystem, uuid, toDir string) error {
	filePath := path.Join(toDir, fmt.Sprintf("%s.json", uuid))
	if err := fs.Remove(filePath); err != nil {
		return errors.Wrapf(err, "failed to remove %s", filePath)
	}
	return nil
}

// WriteCSV writes out the passed in records as CSV content to the passed in
// io.Writer.
func WriteCSV(fileWriter io.Writer, records [][]string) (err error) {
	writer := csv.NewWriter(fileWriter)
	for _, record := range records {
		if err = writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

// WriteJobSummary writes out a CSV summary of the passed in *model.Job to a
// file called "JobSummary.csv" in the provided output directory.
func WriteJobSummary(fs FileSystem, outputDir string, job *model.Job) error {
	outputPath := path.Join(outputDir, "JobSummary.csv")
	fileWriter, err := fs.Create(outputPath)
	if err != nil {
		return err
	}
	defer fileWriter.Close()
	records := [][]string{
		{"Job ID", job.InvocationID},
		{"Job Name", job.Name},
		{"Application ID", job.AppID},
		{"Application Name", job.AppName},
		{"Submitted By", job.Submitter},
	}
	return WriteCSV(fileWriter, records)
}

// stepToRecord converts a *model.Step to a [][]string so it can be turned into
// part of a CSV file.
func stepToRecord(step *model.Step) [][]string {
	var retval [][]string
	params := step.Config.Parameters()
	for _, p := range params {
		retval = append(retval, []string{
			step.Executable(),
			p.Name,
			p.Value,
		})
	}
	return retval
}

// WriteJobParameters writes out the *model.Job's parameters to a CSV file
// called "JobParameters.csv" located in the output directory.
func WriteJobParameters(fs FileSystem, outputDir string, job *model.Job) error {
	outputPath := path.Join(outputDir, "JobParameters.csv")
	fileWriter, err := fs.Create(outputPath)
	if err != nil {
		return err
	}
	defer fileWriter.Close()
	records := [][]string{
		{"Executable", "Argument Option", "Argument Value"},
	}
	for _, s := range job.Steps {
		stepRecords := stepToRecord(&s)
		for _, sr := range stepRecords {
			records = append(records, sr)
		}
	}
	return WriteCSV(fileWriter, records)
}
