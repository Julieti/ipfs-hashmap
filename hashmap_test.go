package hashmap

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"
	"zly.ecnu.edu.cn/hashmap/ipfs"
)

func TestCreate(t *testing.T)  {
	words := loadTestFile("/Users/leah/Downloads/text/5305.txt")
	m := New()
	hash := make([]uint32, 0)

	nowTime := time.Now()
	for k, v := range words {
		hash = Hash(k)
		m.Put(k, v, 0, hash)
	}

	// Test get
	//for k := range words {
	//	hash = Hash(k)
	//	fmt.Print(k + " ")
	//	value, found := m.Get(k, 0, hash)
	//
	//	if found {
	//		fmt.Println( value)
	//	} else {
	//		break
	//	}
	//}

	fmt.Println(m.Traversal())

	//fmt.Println(len(m.m))

	fmt.Println(time.Now().Sub(nowTime))

}

func loadTestFile(path string) map[string]string {
	words := make(map[string]string, 0)
	file, err := os.Open(path)
	if err != nil {
		panic("Couldn't open " + path)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		if line, err := reader.ReadBytes(byte('\n')); err != nil {
			break
		} else {
			if len(line) > 0 {
				parts := strings.Split(string(line)," ")
				words[parts[0]] = parts[1]
			}
		}
	}
	return words
}

func TestGet(t *testing.T)  {
	//keys := make([]string, 0)
	//cid := "QmSiW9WKhTYpMYNvKGJoMFFj759R5pv9F5wFk6M19raEFa" //300词 4kb
	//cid := "QmNTBfwaMPcPKX5GuerjHyBz16W5EFvgx6wrRGNPqTFdEc" //5000词 4kb 49m32.46607397s
	//cid := "Qmb8X33H22efZtjyTugf21jXzUaBEjYJgp6B5k31bEBK56" // 5000词 256kb  34m27.51896108s
	//cid := "QmSkmmg8ZFX4QqTjpGPS81xCtWXHaFY2Zh6wzafsZAMkMj" // 5000词 256kb 12 8 25m22.389227552s
	//cid := "Qmea3VtX9LT6gcoqeeQmYiW91GsqtLNYzRUMiCDrE8aeNZ" //300
	//cid := "Qmbj5mJwSw2PvBRP4oU786mZL6HxMWDdVGAVLajH9jxJAb" //5000
	cid := "QmW2xTfMT26VwkxhvG1Z2KtH4EzmNXTceAw7Mx3gmYU8LB"

	//cid := ""
	words := loadTestFile("/Users/leah/Downloads/text/5305.txt")
	nowTime := time.Now()

	for k := range words {
		fmt.Println(k)
		hash := Hash(k)
		value, pathList, levelList := Get(k, cid, hash, 0)
		if value == "" {
			break
		}
		fmt.Print(value)
		fmt.Println(pathList)
		fmt.Println(levelList)
	}

	//hash := Hash("because")
	//value, pathList, levelList := Get("because", cid, hash, 0)
	//
	//fmt.Println(value)
	//fmt.Println(pathList)
	//fmt.Println(levelList)

	fmt.Println(time.Now().Sub(nowTime))
}

func TestHash(t *testing.T)  {
	fmt.Println(Hash("bring"))
	fmt.Println(Hash("four"))
	fmt.Println(Hash("leg"))
	fmt.Println(Hash("man"))
	fmt.Println(Hash("pig"))
}

func TestUint2String(t *testing.T)  {
	//var value interface{}
	//
	//value = 3457890
	//
	//fmt.Println(strconv.Itoa(int(value.(uint32))))
	fmt.Println(len("QmbRPWB6RMPzHGjVM5ne2UNmzGPXCmo96pxGWHhrFvk1Za,0.0018850141\n"))
}

func TestUpload(t *testing.T)  {
	lines := make([]string, 0)
	f, err := os.Open("/Users/leah/Public/dev/scala/search-maven/src/main/scala/work/analyzed.entries.json.seq")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n') //以'\n'为结束符读入一行

		if err != nil || io.EOF == err {
			break
		}
		lines = append(lines, line)
	}

	start := time.Now()

	for _, l := range lines {
		fmt.Println(ipfs.UploadIndex(l))
	}

	fmt.Println(time.Now().Sub(start))
}

func TestUpdate(t *testing.T) {
	cid := "QmSiW9WKhTYpMYNvKGJoMFFj759R5pv9F5wFk6M19raEFa"

	words := loadTestFile("/Users/leah/Downloads/words.txt")

	fmt.Println(Update(cid, words))

}