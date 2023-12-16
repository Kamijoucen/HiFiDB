package file

import (
	"fmt"
	"os"
	"testing"
)

func Test_io(t *testing.T) {

	filename := "/home/kamijoucen/test.txt"

	file, _ := os.Create(filename)
	defer fclose(file)

	_, _ = file.WriteString("lisicen io test")

	file2, _ := os.Open(filename)

	bytes := make([]byte, 1024)

	n, _ := file2.Read(bytes)

	fmt.Printf("%s", bytes[:n])

	os.Remove(filename)
}

func Test_randomIO(t *testing.T) {

	file, _ := os.Create("/home/kamijoucen/t1.txt")
	defer fclose(file)

	file.WriteString("1234567890")

	bytes := make([]byte, 1)
	file.ReadAt(bytes, 1)

	os.Remove("/home/kamijoucen/t1.txt")

	fmt.Printf("%s", bytes)
}

func fclose(file *os.File) {
	err := file.Close()
	if err != nil {
		panic(err)
	}
}
