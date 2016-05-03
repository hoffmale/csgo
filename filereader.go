package csgo

import (
	"bufio"
	"io"
	"os"
	"strings"
)

// FileReader is a helper struct for reading text files
type FileReader struct {
	fileHandle   *os.File
	reader       *bufio.Reader
	EOFReached   bool
	currentLine  string
	currentError error
	currentEOF   bool
}

func (file *FileReader) internalReadLine() (line string, eofReached bool, errMsg error) {
	eofReached = false
	line, errMsg = file.reader.ReadString('\n')

	if errMsg == io.EOF {
		eofReached = true
		errMsg = nil
	}

	line = strings.TrimRight(line, "\r\n")

	return line, eofReached, errMsg
}

// CreateFileReader opens a text file
func CreateFileReader(path string) (*FileReader, error) {
	file := &FileReader{EOFReached: false, currentLine: ""}
	var errMsg error

	file.fileHandle, errMsg = os.Open(path)
	if errMsg != nil {
		return nil, errMsg
	}

	// get a reader with a considerably big buffer
	file.reader = bufio.NewReaderSize(file.fileHandle, 65535)

	file.ReadLine()

	if (file.currentEOF && file.currentLine == "") || file.currentError != nil {
		file.EOFReached = true
	}

	return file, nil
}

// Close the text file
func (file *FileReader) Close() {
	file.fileHandle.Close()
	file.EOFReached = true
	file.currentLine = ""
	file.currentError = io.EOF
}

// ReadLine reads the next line of the text file (includes handling for empty lines and different line endings)
func (file *FileReader) ReadLine() (string, error) {
	retLine := file.currentLine
	retError := file.currentError

	if !file.currentEOF && file.currentError == nil {
		var line string
		var eofReached bool
		var errMsg error

		for line, eofReached, errMsg = file.internalReadLine(); !eofReached && line == "" && errMsg == nil; line, eofReached, errMsg = file.internalReadLine() {
		}

		file.currentEOF = file.currentEOF || eofReached
		file.currentLine = line
		file.currentError = errMsg

		if (line == "" && eofReached) || errMsg != nil {
			file.EOFReached = true
		}
	} else {
		file.EOFReached = true
	}

	return retLine, retError
}
