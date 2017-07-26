package fs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/cyverse-de/model"
)

type testFS struct {
	filemap    map[string]*byteFile
	failOpen   bool
	failCreate bool
	failRemove bool
	failFile   bool
}

func newTestFS() *testFS {
	return &testFS{
		filemap: map[string]*byteFile{},
	}
}

type byteFile struct {
	fail bool
	*bytes.Buffer
}

func newByteFile(fail bool) *byteFile {
	return &byteFile{
		fail,
		bytes.NewBuffer([]byte{}),
	}
}

func (b *byteFile) Close() error {
	return nil
}

func (b *byteFile) Read(p []byte) (int, error) {
	if b.fail {
		return 0, errors.New("read error")
	}
	return b.Buffer.Read(p)
}

func (b *byteFile) Write(p []byte) (int, error) {
	if b.fail {
		return 0, errors.New("write error")
	}
	return b.Buffer.Write(p)
}

func (t *testFS) Open(path string) (File, error) {
	if t.failOpen {
		return nil, errors.New("failOpen was true")
	}
	if _, ok := t.filemap[path]; !ok {
		return nil, errors.New("failed to open file")
	}
	return t.filemap[path], nil
}

func (t *testFS) Create(path string) (File, error) {
	if t.failCreate {
		return nil, errors.New("failed to create file")
	}
	t.filemap[path] = newByteFile(t.failFile)
	return t.filemap[path], nil
}

func (t *testFS) Remove(path string) error {
	if t.failRemove {
		return errors.New("failed to remove file")
	}
	delete(t.filemap, path)
	return nil
}

func TestCopyJobFile(t *testing.T) {
	tfs := newTestFS()
	uuid := "00000000-0000-0000-0000-000000000000"
	from := path.Join("test", fmt.Sprintf("%s.json", uuid))
	c, err := tfs.Create(from)
	if err != nil {
		t.Error(err)
	}
	c.Write([]byte("this is a test"))
	to := "/tmp"
	err = CopyJobFile(tfs, uuid, from, to)
	if err != nil {
		t.Error(err)
	}
	tmpPath := path.Join(to, fmt.Sprintf("%s.json", uuid))
	if _, err = tfs.Open(tmpPath); err != nil {
		t.Error(err)
	}

	// test failures from the Open() function
	tfs = newTestFS()
	tfs.failOpen = true
	err = CopyJobFile(tfs, uuid, from, to)
	if err == nil {
		t.Error(err)
	}

	// test failures from the Create() function.
	tfs = newTestFS()
	c, err = tfs.Create(from)
	if err != nil {
		t.Error(err)
	}
	c.Write([]byte("this is a test"))
	tfs.failCreate = true
	err = CopyJobFile(tfs, uuid, from, to)
	if err == nil {
		t.Error(err)
	}

	// test failures from the Copy() function.
	tfs = newTestFS()
	c, err = tfs.Create(from)
	if err != nil {
		t.Error(err)
	}
	c.Write([]byte("this is a test"))
	tfs.failFile = true
	err = CopyJobFile(tfs, uuid, from, to)
	if err == nil {
		t.Error(err)
	}
}

func TestDeleteJobFile(t *testing.T) {
	tfs := newTestFS()
	uuid := "00000000-0000-0000-0000-000000000000"
	from := path.Join("test", fmt.Sprintf("%s.json", uuid))
	c, err := tfs.Create(from)
	if err != nil {
		t.Error(err)
	}
	c.Write([]byte("this is a test"))
	to := "/tmp"
	err = CopyJobFile(tfs, uuid, from, to)
	if err != nil {
		t.Error(err)
	}
	DeleteJobFile(tfs, uuid, to)
	tmpPath := path.Join(to, fmt.Sprintf("%s.json", uuid))
	if _, err := os.Open(tmpPath); err == nil {
		t.Errorf("tmpPath %s existed after deleteJobFile() was called", tmpPath)
	}
}

func TestDeleteJobFileFail(t *testing.T) {
	tfs := newTestFS()
	tfs.failRemove = true
	uuid := "00000000-0000-0000-0000-000000000000"
	from := path.Join("test", fmt.Sprintf("%s.json", uuid))
	c, err := tfs.Create(from)
	if err != nil {
		t.Error(err)
	}
	c.Write([]byte("this is a test"))
	to := "/tmp"
	err = CopyJobFile(tfs, uuid, from, to)
	if err != nil {
		t.Error(err)
	}
	err = DeleteJobFile(tfs, uuid, to)
	if err == nil {
		t.Error("err was nil")
	}
}

func TestWriteCSV(t *testing.T) {
	expected := `test0,test0,test0
test1,test1,test1
test2,test2,test2
`
	records := [][]string{
		{"test0", "test0", "test0"},
		{"test1", "test1", "test1"},
		{"test2", "test2", "test2"},
	}
	buf := bytes.NewBuffer([]byte{})
	if err := WriteCSV(buf, records); err != nil {
		t.Error(err)
	}
	actual := string(buf.Bytes())
	if actual != expected {
		t.Errorf("Contents of csv were:\n%s\n\tinstead of:\n%s\n", actual, expected)
	}
}

func TestWriteJobSummary(t *testing.T) {
	tfs := newTestFS()
	j := &model.Job{
		InvocationID: "07b04ce2-7757-4b21-9e15-0b4c2f44be26",
		Name:         "Echo_test",
		AppID:        "c7f05682-23c8-4182-b9a2-e09650a5f49b",
		AppName:      "Word Count",
		Submitter:    "test_this_is_a_test",
	}
	expected := `Job ID,07b04ce2-7757-4b21-9e15-0b4c2f44be26
Job Name,Echo_test
Application ID,c7f05682-23c8-4182-b9a2-e09650a5f49b
Application Name,Word Count
Submitted By,test_this_is_a_test
`
	if err := WriteJobSummary(tfs, "test", j); err != nil {
		t.Error(err)
	}
	outPath := "test/JobSummary.csv"
	inputreader, err := tfs.Open(outPath)
	if err != nil {
		t.Error(err)
	}
	buf := bytes.NewBuffer([]byte{})
	_, err = io.Copy(buf, inputreader)
	if err != nil {
		t.Error(err)
	}
	actual := buf.String()
	if actual != expected {
		t.Errorf("Contents of %s were:\n%s\n\tinstead of:\n%s\n", outPath, actual, expected)
	}
}

func TestStepToRecord(t *testing.T) {
	step := &model.Step{
		Component: model.StepComponent{
			Location: "/this/is/a/location",
			Name:     "test-name",
		},
		Config: model.StepConfig{
			Params: []model.StepParam{
				{
					Name:  "parameter-name",
					Value: "This is a test",
				},
			},
		},
	}
	actual := stepToRecord(step)
	expected := [][]string{
		{"", "parameter-name", "This is a test"},
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Record %#v does not equal %#v", actual, expected)
	}
}

func TestWriteJobParameters(t *testing.T) {
	tfs := newTestFS()
	j := &model.Job{
		Steps: []model.Step{
			{
				Component: model.StepComponent{
					Location: "/this/is/a/location",
					Name:     "test-name",
				},
				Config: model.StepConfig{
					Params: []model.StepParam{
						{
							Name:  "parameter-name",
							Value: "This is a test",
						},
					},
				},
			},
		},
	}
	expected := `Executable,Argument Option,Argument Value
,parameter-name,This is a test
`
	if err := WriteJobParameters(tfs, "test", j); err != nil {
		t.Error(err)
	}
	outPath := "test/JobParameters.csv"
	inputreader, err := tfs.Open(outPath)
	if err != nil {
		t.Error(err)
	}
	buf := bytes.NewBuffer([]byte{})
	_, err = io.Copy(buf, inputreader)
	if err != nil {
		t.Error(err)
	}
	actual := buf.String()
	if actual != expected {
		t.Errorf("Contents of %s were:\n%s\n\tinstead of:\n%s\n", outPath, actual, expected)
	}
}
