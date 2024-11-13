package main

import (
	"fmt"
	"os"
)

func PanicIfError(err error, message ...string) {
	if err != nil {
		if len(message) == 1 {
			panic(fmt.Errorf(message[0]+": %w", err))
		}

		panic(err)
	}
}

func CreateTemporaryFile(prefix string) (file *os.File, err error) {
	tempFile, err := os.CreateTemp("", prefix)
	PanicIfError(err)

	return tempFile, nil
}

func DeleteTemporaryFile(file *os.File) {
	os.Remove(file.Name())
}

func IntToString(i int) string {
	return fmt.Sprintf("%d", i)
}
