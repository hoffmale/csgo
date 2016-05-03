package csgo

import (
	"os"
	"testing"
)

const TESTFILE = "_testing.csv"

func writeTestFile(path string, content string) error {
	os.Remove(path)
	file, errMsg := os.Create(path)

	if errMsg != nil {
		return errMsg
	}

	file.WriteString(content)
	file.Close()

	return nil
}

func checkWriteTestFile(t *testing.T, content string) bool {
	if errMsg := writeTestFile(TESTFILE, content); errMsg != nil {
		t.Errorf("couldnt open test file: %v", errMsg)
		t.Fail()
		return false
	}
	return true
}

func checkOpenTestFile(t *testing.T, fileOutput **FileReader) bool {
	file, errMsg := CreateFileReader(TESTFILE)
	if file == nil || errMsg != nil {
		t.Errorf("failure upon opening test file: %v", errMsg)
		t.Fail()
		return false
	}

	*fileOutput = file

	return true
}

func checkEOFNotReached(t *testing.T, file *FileReader) bool {
	if file.EOFReached {
		t.Errorf("unexpected EOF reached")
		t.Fail()
		return false
	}
	return true
}

func checkEOFReached(t *testing.T, file *FileReader) bool {
	if !file.EOFReached {
		t.Errorf("expected EOF was not reached")
		t.Fail()
		return false
	}
	return true
}

func checkLine(t *testing.T, file *FileReader, content string) bool {
	success := checkEOFNotReached(t, file)

	if success {
		if line, errMsg := file.ReadLine(); line != content || errMsg != nil {
			t.Errorf("error reading line: got '%s', expected '%s' (eof: %v)", line, content, file.EOFReached)
			if errMsg != nil {
				t.Error(errMsg)
			}
			t.Fail()
			success = false
		}
	}
	return success
}

func TestOpenCSVFile_NotExisting(t *testing.T) {
	os.Remove(TESTFILE)
	// file not existing
	file, errMsg := CreateFileReader(TESTFILE)
	if file != nil || errMsg == nil {
		t.Errorf("expected nil file and 'FileNotFound' error, got %p file and '%v' error msg", file, errMsg)
		t.Fail()
	}
}

func TestOpenCSVFile_Empty1(t *testing.T) {
	// file exists, but empty
	var file *FileReader

	success := checkWriteTestFile(t, "") && checkOpenTestFile(t, &file)
	if success {
		checkEOFReached(t, file)
		file.Close()
	}
}

func TestOpenCSVFile_Empty2(t *testing.T) {
	// file exists, but empty
	var file *FileReader

	success := checkWriteTestFile(t, "\n") && checkOpenTestFile(t, &file)
	if success {
		checkEOFReached(t, file)
		file.Close()
	}
}

func TestOpenCSVFile_Empty3(t *testing.T) {
	// file exists, but empty
	var file *FileReader

	success := checkWriteTestFile(t, "\r\n") && checkOpenTestFile(t, &file)
	if success {
		checkEOFReached(t, file)
		file.Close()
	}
}

func TestOpenCSVFile_Empty4(t *testing.T) {
	// file exists, but empty
	var file *FileReader

	success := checkWriteTestFile(t, "\n\n\n") && checkOpenTestFile(t, &file)
	if success {
		checkEOFReached(t, file)
		file.Close()
	}
}

func TestOpenCSVFile_NotEmpty(t *testing.T) {
	// file exists, with data!
	var file *FileReader

	success := checkWriteTestFile(t, "a,1,5.0\nb,2,4.5\nc,3,4.0\nd,4,3.5\ne,5,3.0") && checkOpenTestFile(t, &file)

	if success {
		checkEOFNotReached(t, file)
		file.Close()
	}
}

func TestCSVFileReadLine(t *testing.T) {
	cases := []struct {
		fileContent string
		lines       []string
	}{
		// compact case
		{fileContent: "a,1,5.0\nb,2,4.5\nc,3,4.0\nd,4,3.5\ne,5,3.0", lines: []string{"a,1,5.0", "b,2,4.5", "c,3,4.0", "d,4,3.5", "e,5,3.0"}},
		// windows line endings
		{fileContent: "a,1,5.0\r\nb,2,4.5\r\nc,3,4.0\r\nd,4,3.5\r\ne,5,3.0", lines: []string{"a,1,5.0", "b,2,4.5", "c,3,4.0", "d,4,3.5", "e,5,3.0"}},
		// trailing newlines
		{fileContent: "a,1,5.0\nb,2,4.5\nc,3,4.0\nd,4,3.5\ne,5,3.0\n", lines: []string{"a,1,5.0", "b,2,4.5", "c,3,4.0", "d,4,3.5", "e,5,3.0"}},
		{fileContent: "a,1,5.0\r\nb,2,4.5\r\nc,3,4.0\r\nd,4,3.5\r\ne,5,3.0\r\n", lines: []string{"a,1,5.0", "b,2,4.5", "c,3,4.0", "d,4,3.5", "e,5,3.0"}},
		// empty lines in between
		{fileContent: "a,1,5.0\nb,2,4.5\n\nc,3,4.0\n\n\nd,4,3.5\ne,5,3.0\n", lines: []string{"a,1,5.0", "b,2,4.5", "c,3,4.0", "d,4,3.5", "e,5,3.0"}},
		{fileContent: "a,1,5.0\r\nb,2,4.5\r\nc,3,4.0\r\n\r\nd,4,3.5\r\n\r\n\r\ne,5,3.0\r\n", lines: []string{"a,1,5.0", "b,2,4.5", "c,3,4.0", "d,4,3.5", "e,5,3.0"}},
		// line ending mixups
		{fileContent: "a,1,5.0\nb,2,4.5\r\nc,3,4.0\r\nd,4,3.5\r\n\ne,5,3.0\r\n", lines: []string{"a,1,5.0", "b,2,4.5", "c,3,4.0", "d,4,3.5", "e,5,3.0"}},
	}

	var file *FileReader

	for _, testcase := range cases {
		success := checkWriteTestFile(t, testcase.fileContent) && checkOpenTestFile(t, &file)

		if !success {
			continue
		}
		defer file.Close()

		for _, line := range testcase.lines {
			success = success && checkLine(t, file, line)
			if !success {
				break
			}
		}

		success = success && checkEOFReached(t, file)
	}
}
