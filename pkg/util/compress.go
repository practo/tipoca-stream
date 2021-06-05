package util

import (
	"compress/gzip"
	"io"
)

// GzipWrite writes gzipped data bytes to the writer
func GzipWrite(w io.Writer, data []byte) error {
	writer, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
	defer writer.Close()
	if err != nil {
		return err
	}

	_, err = writer.Write(data)
	if err != nil {
		return err
	}

	return nil
}
