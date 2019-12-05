package hashmap

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	version := &VersionNote{}

	nowTime := time.Now()
	for k, v := range words {
		hash = Hash(k)
		m.Put(k, v, 0, hash)
	}

	cid := m.Traversal()
	version.Root = cid
	version.Date = time.Now().Format("2006-01-02 15:04:05")
	version.DocumentCount = 1
	version.WordCount = len(words)

	in, _ := json.Marshal(version)
	cid = ipfs.UploadIndex(string(in))

	fmt.Println(cid)
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
	//cid := "QmYjqLnRB7iBD5fbvcdeKPoXw4s9Uq5RMJp7pHGxq6EAa6" 5000 + 300
	//cid := "QmNaAZu4nMvhPL1HL2BR6y7GhWqkcYJX8YoKznpL4x8h4j" //300 + 5000
	//cid := "QmPLp1er7MtADCX1Y2zMWipVF3fivtKmi7joosPo7zcNC6"//300+10000
	cid := "QmRNQUKvz7PAg72yj8PQ6tEP6pDStLGk68MQe2ZkBNVoKM"

	//cid := ""
	nowTime := time.Now()

	words := loadTestFile("/Users/leah/Downloads/text/5305.txt")
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

	//hash := Hash("isadoramoretti")
	//value, pathList, levelList := Get("isadoramoretti", cid, hash, 0)
	//
	//fmt.Println(value)
	//fmt.Println(pathList)
	//fmt.Println(levelList)

	fmt.Println(time.Now().Sub(nowTime))
}

func TestHash(t *testing.T)  {
	fmt.Println(Hash("spoon"))
}

func TestUint2String(t *testing.T)  {
	//var value interface{}
	//
	//value = 3457890
	//
	//fmt.Println(strconv.Itoa(int(value.(uint32))))
	ks := make([]string, 0)
	ks = append(ks, "1")
	ks = append(ks, "2")
	ks = append(ks, "3")
	ks = append(ks, "4")
	ls := make([]string, 0)
	ks = append(ks[:3], "5")

	ls = append(ls ,ks...)
	fmt.Println(ls)
	fmt.Println(ks)
	//fmt.Println(len("QmbRPWB6RMPzHGjVM5ne2UNmzGPXCmo96pxGWHhrFvk1Za,0.0018850141\n"))
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
	//cid := "QmNhjPf3JSQWpzfdi62vVfqQgk95JopJDEJwmMRxRuHb3k"//5000词
	cid := "QmSng8oveErXYMYzNrFSoGzSU7aQw3Lg4ssvgMQ1bs8SHw"
	w := make(map[string]*word2Hash)
	keys := make(map[string]string, 0)

	words := loadTestFile("/Users/leah/Downloads/text/11844.txt")

	for k := range words {
		keys[k] = ""
		h := make([]uint32, 0)
		h = Hash(k)
		w2H := &word2Hash{
			hash: h,
		}
		w[k] = w2H
	}

	fmt.Println(Update(cid, words, w, keys))

}

func TestVersionNote(t *testing.T)  {
	v1 := "QmVe9JUMKPngTHXmHhK9zm2S5i3j6X9bSpYdft7XruaP2y"
	v2 := "QmSJLKSRzVdwXgwsniX9BxMKepojKdmsLSP9ZbfkQwABai"
	newRoot := ""
	r1 := &VersionNote{}
	content := ipfs.CatIndex(v1)
	err := json.Unmarshal([]byte(content), &r1)

	r2 := &VersionNote{}
	content = ipfs.CatIndex(v2)
	err = json.Unmarshal([]byte(content), &r2)

	if err != nil {
		log.Printf("version merge error: unmarshal error: %v", err)
	}

	if r1.WordCount > r2.WordCount {
		newRoot = VersionMerge(r1.Root, r2.Root, 0)
	} else {
		newRoot = VersionMerge(r2.Root, r1.Root, 0)
	}

	r1.WordCount = r1.WordCount + r2.WordCount
	r1.DocumentCount = r1.DocumentCount + r2.DocumentCount
	r1.Date = time.Now().Format("2006-01-02 15:04:05")
	r1.Root = newRoot

	in, _ := json.Marshal(r1)
	cid := ipfs.UploadIndex(string(in))

	fmt.Println(cid)

	//QmbtWSHHARBHDsMvSktqTV3R82oSYaDw9dcqFUHuY5Tv3B
}