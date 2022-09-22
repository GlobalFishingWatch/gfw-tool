package common

import (
	"log"
	"os"
)

func WriteFileFromString(path string, filename string, content string) {
	CreateDirectory(path)
	log.Printf("→ OS →→  Creating local file with [%s/%s]", path, filename)
	f, err := os.Create(path + "/" + filename)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	_, err2 := f.WriteString(content)

	if err2 != nil {
		log.Fatal(err2)
	}
}

func CreateDirectory(path string) {
	log.Printf("→ OS →→ Creating path [%s] if not exists", path)
	_ = os.Mkdir(path, os.ModePerm)
}
