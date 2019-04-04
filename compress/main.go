package main

import (
	"bytes"
	"compress/flate"
	"fmt"
)

func main() {
	s := "username@whatever.com:P@ssw0rd"
	fmt.Printf("In Length: %d\n", len(s))
	o := comp(s)
	fmt.Printf("Out Length: %d\n", len(o))
}

func comp(in string) (out []byte) {
	var b bytes.Buffer
	w, err := flate.NewWriter(&b, 9)
	if err != nil {
		panic(err)
	}
	w.Write([]byte(in))
	w.Close()
	return b.Bytes()
}
