package ipfs

import (
	"bytes"
	"github.com/ipfs/go-ipfs-api"
	"io/ioutil"
	"log"
	"os"
)

var sh *shell.Shell

func init()  {
	sh = shell.NewShell("localhost:5002")
}

// Upload original file
func UploadFile(filePath string) string {
	//  Where your local node is running on localhost:5001
	//  export IPFS_PATH=/Users/leah/Downloads/ipfs
	file, err := os.Open(filePath)
	cid, err := sh.Add(file)
	if err != nil {
		log.Printf("Upload file error: %s", err)
		os.Exit(1)
	}
	defer file.Close()

	return cid
}


func UploadIndex(indexText string) string {
	hash, err := sh.Add(bytes.NewBufferString(indexText))
	if err != nil {
		log.Printf("Upload index error: %s", err)
		os.Exit(1)
	}

	//command := "echo '" + indexText + "'| ipfs add --raw-leaves"
	//cmd := exec.Command("/bin/bash", "-c", command)
	//bytes,err := cmd.Output()
	//if err != nil {
	//	log.Println(err)
	//}
	//resp := string(bytes)
	//s := strings.Split(resp, " ")
	return hash
}

func CatIndex(indexHash string) string {
	read, err := sh.Cat(indexHash)
	if err != nil {
		log.Printf("Cat index error: %s", err)
		os.Exit(1)
	}
	body, err := ioutil.ReadAll(read)

	if err != nil {
		log.Printf("Read index error: %s", err)
		os.Exit(1)
	}

	return string(body)
}



