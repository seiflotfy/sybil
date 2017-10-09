package pkg

import "fmt"

import "os"
import "strings"
import "encoding/gob"
import "compress/gzip"

type fileDecoder struct {
	*gob.Decoder
	File *os.File
}

func decodeInto(filename string, obj interface{}) error {
	dec := GetFileDecoder(filename)
	defer dec.File.Close()

	err := dec.Decode(obj)
	return err
}

func getCompressedDecoder(filename string) fileDecoder {

	var dec *gob.Decoder

	file, err := os.Open(filename)
	if err != nil {
		Debug("COULDNT OPEN GZ", filename)
		return fileDecoder{gob.NewDecoder(file), file}
	}

	reader, err := gzip.NewReader(file)
	if err != nil {
		Debug("COULDNT DECOMPRESS GZ", filename)
		return fileDecoder{gob.NewDecoder(reader), file}
	}

	dec = gob.NewDecoder(reader)
	return fileDecoder{dec, file}
}

func GetFileDecoder(filename string) *fileDecoder {
	// if the file ends with GZ ext, we use compressed decoder
	if strings.HasSuffix(filename, GZIP_EXT) {
		dec := getCompressedDecoder(filename)
		return &dec
	}

	file, err := os.Open(filename)
	// if we try to open the file and its missing, maybe there is a .gz version of it
	if err != nil {
		zfilename := fmt.Sprintf("%s%s", filename, GZIP_EXT)
		_, err = os.Open(zfilename)

		// if we can open this file, we return compressed file decoder
		if err == nil {
			dec := getCompressedDecoder(zfilename)
			return &dec
		}
	}

	// otherwise, we just return vanilla decoder for this file
	dec := fileDecoder{gob.NewDecoder(file), file}
	return &dec

}
