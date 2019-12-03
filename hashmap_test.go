// Copyright (c) 2015, Emir Pasic. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hashmap

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"zly.ecnu.edu.cn/hashmap/ipfs"
)

//func TestMapPut(t *testing.T) {
//	m := New()
//	m.Put(5, "e")
//	m.Put(6, "f")
//	m.Put(7, "g")
//	m.Put(3, "c")
//	m.Put(4, "d")
//	m.Put(1, "x")
//	m.Put(2, "b")
//	m.Put(1, "a") //overwrite
//
//	if actualValue := m.Size(); actualValue != 7 {
//		t.Errorf("Got %v expected %v", actualValue, 7)
//	}
//	if actualValue, expectedValue := m.Keys(), []interface{}{1, 2, 3, 4, 5, 6, 7}; !sameElements(actualValue, expectedValue) {
//		t.Errorf("Got %v expected %v", actualValue, expectedValue)
//	}
//	if actualValue, expectedValue := m.Values(), []interface{}{"a", "b", "c", "d", "e", "f", "g"}; !sameElements(actualValue, expectedValue) {
//		t.Errorf("Got %v expected %v", actualValue, expectedValue)
//	}
//
//	// key,expectedValue,expectedFound
//	tests1 := [][]interface{}{
//		{1, "a", true},
//		{2, "b", true},
//		{3, "c", true},
//		{4, "d", true},
//		{5, "e", true},
//		{6, "f", true},
//		{7, "g", true},
//		{8, nil, false},
//	}
//
//	for _, test := range tests1 {
//		// retrievals
//		actualValue, actualFound := m.Get(test[0])
//		if actualValue != test[1] || actualFound != test[2] {
//			t.Errorf("Got %v expected %v", actualValue, test[1])
//		}
//	}
//}

//func TestMapRemove(t *testing.T) {
//	m := New()
//	m.Put(5, "e")
//	m.Put(6, "f")
//	m.Put(7, "g")
//	m.Put(3, "c")
//	m.Put(4, "d")
//	m.Put(1, "x")
//	m.Put(2, "b")
//	m.Put(1, "a") //overwrite
//
//	m.Remove(5)
//	m.Remove(6)
//	m.Remove(7)
//	m.Remove(8)
//	m.Remove(5)
//
//	if actualValue, expectedValue := m.Keys(), []interface{}{1, 2, 3, 4}; !sameElements(actualValue, expectedValue) {
//		t.Errorf("Got %v expected %v", actualValue, expectedValue)
//	}
//
//	if actualValue, expectedValue := m.Values(), []interface{}{"a", "b", "c", "d"}; !sameElements(actualValue, expectedValue) {
//		t.Errorf("Got %v expected %v", actualValue, expectedValue)
//	}
//	if actualValue := m.Size(); actualValue != 4 {
//		t.Errorf("Got %v expected %v", actualValue, 4)
//	}
//
//	tests2 := [][]interface{}{
//		{1, "a", true},
//		{2, "b", true},
//		{3, "c", true},
//		{4, "d", true},
//		{5, nil, false},
//		{6, nil, false},
//		{7, nil, false},
//		{8, nil, false},
//	}
//
//	for _, test := range tests2 {
//		actualValue, actualFound := m.Get(test[0])
//		if actualValue != test[1] || actualFound != test[2] {
//			t.Errorf("Got %v expected %v", actualValue, test[1])
//		}
//	}
//
//	m.Remove(1)
//	m.Remove(4)
//	m.Remove(2)
//	m.Remove(3)
//	m.Remove(2)
//	m.Remove(2)
//
//	if actualValue, expectedValue := fmt.Sprintf("%s", m.Keys()), "[]"; actualValue != expectedValue {
//		t.Errorf("Got %v expected %v", actualValue, expectedValue)
//	}
//	if actualValue, expectedValue := fmt.Sprintf("%s", m.Values()), "[]"; actualValue != expectedValue {
//		t.Errorf("Got %v expected %v", actualValue, expectedValue)
//	}
//	if actualValue := m.Size(); actualValue != 0 {
//		t.Errorf("Got %v expected %v", actualValue, 0)
//	}
//	if actualValue := m.Empty(); actualValue != true {
//		t.Errorf("Got %v expected %v", actualValue, true)
//	}
//}

//func TestMapSerialization(t *testing.T) {
//	m := New()
//	m.Put("a", 1.0)
//	m.Put("b", 2.0)
//	m.Put("c", 3.0)
//
//	var err error
//	assert := func() {
//		if actualValue, expectedValue := m.Keys(), []interface{}{"a", "b", "c"}; !sameElements(actualValue, expectedValue) {
//			t.Errorf("Got %v expected %v", actualValue, expectedValue)
//		}
//		if actualValue, expectedValue := m.Values(), []interface{}{1.0, 2.0, 3.0}; !sameElements(actualValue, expectedValue) {
//			t.Errorf("Got %v expected %v", actualValue, expectedValue)
//		}
//		if actualValue, expectedValue := m.Size(), 3; actualValue != expectedValue {
//			t.Errorf("Got %v expected %v", actualValue, expectedValue)
//		}
//		if err != nil {
//			t.Errorf("Got error %v", err)
//		}
//	}
//
//	assert()
//
//	json, err := m.ToJSON()
//	assert()
//
//	err = m.FromJSON(json)
//	assert()
//}

func sameElements(a []interface{}, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for _, av := range a {
		found := false
		for _, bv := range b {
			if av == bv {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

//func benchmarkGet(b *testing.B, m *Map, size int) {
//	for i := 0; i < b.N; i++ {
//		for n := 0; n < size; n++ {
//			m.Get(n)
//		}
//	}
//}

//func benchmarkPut(b *testing.B, m *Map, size int) {
//	for i := 0; i < b.N; i++ {
//		for n := 0; n < size; n++ {
//			m.Put(n, struct{}{})
//		}
//	}
//}

//func benchmarkRemove(b *testing.B, m *Map, size int) {
//	for i := 0; i < b.N; i++ {
//		for n := 0; n < size; n++ {
//			m.Remove(n)
//		}
//	}
//}

//func BenchmarkHashMapGet100(b *testing.B) {
//	b.StopTimer()
//	size := 100
//	m := New()
//	for n := 0; n < size; n++ {
//		m.Put(n, struct{}{})
//	}
//	b.StartTimer()
//	benchmarkGet(b, m, size)
//}

//func BenchmarkHashMapGet1000(b *testing.B) {
//	b.StopTimer()
//	size := 1000
//	m := New()
//	for n := 0; n < size; n++ {
//		m.Put(n, struct{}{})
//	}
//	b.StartTimer()
//	benchmarkGet(b, m, size)
//}

//func BenchmarkHashMapGet10000(b *testing.B) {
//	b.StopTimer()
//	size := 10000
//	m := New()
//	for n := 0; n < size; n++ {
//		m.Put(n, struct{}{})
//	}
//	b.StartTimer()
//	benchmarkGet(b, m, size)
//}

//func BenchmarkHashMapGet100000(b *testing.B) {
//	b.StopTimer()
//	size := 100000
//	m := New()
//	for n := 0; n < size; n++ {
//		m.Put(n, struct{}{})
//	}
//	b.StartTimer()
//	benchmarkGet(b, m, size)
//}

//func BenchmarkHashMapPut100(b *testing.B) {
//	b.StopTimer()
//	size := 100
//	m := New()
//	b.StartTimer()
//	benchmarkPut(b, m, size)
//}

//func BenchmarkHashMapPut1000(b *testing.B) {
//	b.StopTimer()
//	size := 1000
//	m := New()
//	for n := 0; n < size; n++ {
//		m.Put(n, struct{}{})
//	}
//	b.StartTimer()
//	benchmarkPut(b, m, size)
//}

//func BenchmarkHashMapPut10000(b *testing.B) {
//	b.StopTimer()
//	size := 10000
//	m := New()
//	for n := 0; n < size; n++ {
//		m.Put(n, struct{}{})
//	}
//	b.StartTimer()
//	benchmarkPut(b, m, size)
//}

//func BenchmarkHashMapPut100000(b *testing.B) {
//	b.StopTimer()
//	size := 100000
//	m := New()
//	for n := 0; n < size; n++ {
//		m.Put(n, struct{}{})
//	}
//	b.StartTimer()
//	benchmarkPut(b, m, size)
//}

//func BenchmarkHashMapRemove100(b *testing.B) {
//	b.StopTimer()
//	size := 100
//	m := New()
//	for n := 0; n < size; n++ {
//		m.Put(n, struct{}{})
//	}
//	b.StartTimer()
//	benchmarkRemove(b, m, size)
//}

//func BenchmarkHashMapRemove1000(b *testing.B) {
//	b.StopTimer()
//	size := 1000
//	m := New()
//	for n := 0; n < size; n++ {
//		m.Put(n, struct{}{})
//	}
//	b.StartTimer()
//	benchmarkRemove(b, m, size)
//}

//func BenchmarkHashMapRemove10000(b *testing.B) {
//	b.StopTimer()
//	size := 10000
//	m := New()
//	for n := 0; n < size; n++ {
//		m.Put(n, struct{}{})
//	}
//	b.StartTimer()
//	benchmarkRemove(b, m, size)
//}

//func BenchmarkHashMapRemove100000(b *testing.B) {
//	b.StopTimer()
//	size := 100000
//	m := New()
//	for n := 0; n < size; n++ {
//		m.Put(n, struct{}{})
//	}
//	b.StartTimer()
//	benchmarkRemove(b, m, size)
//}

func TestCreate(t *testing.T)  {
	words := loadTestFile("/Users/leah/Downloads/text/42069.txt")
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
	cid := "QmauoJs5VE8XYuVcL1DddwKJqCHEffqyUuHuUA4Fw2giRU"

	//cid := ""
	words := loadTestFile("/Users/leah/Downloads/words.txt")
	nowTime := time.Now()

	for k := range words {
		fmt.Println(k)
		hash := Hash(k)
		value, pathList, levelList :=Get(k, cid, hash, 0)
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
	var value interface{}

	value = 3457890

	fmt.Println(strconv.Itoa(int(value.(uint32))))
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