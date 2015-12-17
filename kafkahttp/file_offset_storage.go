package kafkahttp

import (
	"fmt"
	"io"
	"os"
)

type FileOffsetStorage struct {
	fileName string
	offset   int64
}

func (s *FileOffsetStorage) Get() (int64, error) {
	return s.offset, nil
}

func (s *FileOffsetStorage) write(offset int64) (writeErr error) {
	fd, err := os.OpenFile(s.fileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func() {
		err = fd.Close()
		if err != nil {
			writeErr = err
		}
	}()

	_, err = fmt.Fprintf(fd, "%d\n", offset)
	if err != nil {
		return err
	}

	return nil
}

func (s *FileOffsetStorage) Commit(offset int64) error {
	err := s.write(offset + 1)
	if err != nil {
		return err
	}

	s.offset = offset + 1
	return nil
}

func NewFileOffsetStorage(topic string, partition int) (OffsetStorage, error) {
	s := &FileOffsetStorage{
		fileName: fmt.Sprintf("./.offsetdb/%s-%d", topic, partition),
	}

	fd, err := os.OpenFile(s.fileName, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = fd.Close()
	}()

	_, err = fmt.Fscanf(fd, "%d", &s.offset)
	if err == io.EOF {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}
