package hashmap

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"zly.ecnu.edu.cn/hashmap/ipfs"
)

const BlockSize = 4096
const MaxKey = BlockSize / 47
const MaxBucketEntry = BlockSize / (10 + 60)
// Map holds the elements in go's native map
type Map struct {
	m map[interface{}]*Bucket
	next map[interface{}]*Map
}

type Bucket struct {
	bucket map[interface{}]string
}

type uploadBucket struct {
	Type int `json:"type"`
	Key []string `json:"key"`
	Value []string `json:"value"`
	Next string `json:"next"`
}

// New instantiates a hash map.
func New() *Map {
	return &Map{
		m: make(map[interface{}]*Bucket),
		next: make(map[interface{}]*Map),
	}
}

// Put inserts element into the map.
func (m *Map) Put(key interface{}, value string, level int, hash []uint32) {
	//fmt.Println(key)
	h := hash[level]
	if m.next[h] == nil {
		if m.m[h] != nil {
			m.m[h].bucket[key] = value
			if len(m.m[h].bucket) > MaxBucketEntry {
				m.next[h] = New()
				for k, v := range m.m[h].bucket {
					hash := Hash(k)
					m.next[h].Put(k, v, level + 1, hash)
					delete(m.m[h].bucket, k)
				}
			}
		} else {
			b := make(map[interface{}]string)
			b[key] = value
			m.m[h] = &Bucket{
				bucket: b,
			}
		}
	} else {
		m := m.next[h]
		m.Put(key, value, level + 1, hash)
	}
}

func Hash(key interface{}) []uint32 {
	newHash := make([]uint32, 0)
	//hash := murmur3.Sum32([]byte(key.(string)))
	hash := GetHash([]byte(key.(string)))
	h0 := (hash & 0xfc000000) >> 20
	h1 := (hash & 0x000fc000) >> 12
	h2 := hash & 0x000000fc

	newHash = append(newHash, h0, h1, h2)
	return newHash
}

func Get(keyWord string, cid string, hash []uint32, level int) (string, []string, []int) {
	paths := make([]string, 0)
	levels := make([]int, 0)
	value := ""

	is := true

	for is {
		result, pathList, levelList, isExit := getALevel(keyWord, cid, hash, level)
		paths = append(paths, pathList...)
		levels = append(levels, levelList...)

		if result != "" && result != "next level"{
			return result, paths, levels
		}

		if result == "next level" {
			paths = paths[:len(paths) - 1]
			levels = levels[:len(levels) - 1]
		}
		value = result
		level = level + 1
		cid = pathList[len(pathList) - 1]
		is = isExit
	}

	return value, paths, levels
}

func getALevel(keyWord string, cid string, hash []uint32, level int) (string, []string, []int, bool)  {
	pathList := make([]string, 0)
	levelList := make([]int, 0)
	value := ""
	next := ""
	isExit := false

	for value == "" {
		pathList = append(pathList, cid)
		levelList = append(levelList, level)
		value, next, isExit = getABlock(keyWord, cid, hash[level])
		if !isExit {
			if value == "no bucket keys" {
				pathList = append(pathList, next)
				levelList = append(levelList, -1)
				value = ""
				break
			}
		}
		cid = next
	}

	if isExit {
		pathList = append(pathList, next)
		levelList = append(levelList, -1)
		return value, pathList, levelList, true
	}

	return "", pathList, levelList, false
}

func getABlock(keyWord string, cid string, h uint32) (string, string, bool) {
	content := ipfs.CatIndex(cid)
	n := &uploadBucket{}
	err := json.Unmarshal([]byte(content), &n)

	if err != nil {
		log.Printf("uploadInner error : unmarshal error: %v", err)
	}

	for i, k := range n.Key {
		if k == strconv.Itoa(int(h)) {
			content = ipfs.CatIndex(n.Value[i])
			newCid := n.Value[i]
			n := &uploadBucket{}
			err = json.Unmarshal([]byte(content), &n)
			if n.Type > 1 {
				for j, key := range n.Key {
					if key == keyWord {
						return n.Value[j], newCid, true
					}
				}
				return "no bucket keys", newCid, false
			} else {
				return "next level", newCid, true
			}
		}
	}

	return "", n.Next, false
}

func (m *Map) Traversal() string {
	return traversal(m)
}

func traversal(m *Map) string {
	rootKeys := make([]string, 0)
	l0Keys := make([]string, 0)
	l0Cids := make([]string, 0)
	result := make([]string, 0)
	cid := ""

	l := 0
	tempKey := make([]string, 0)
	templ0Key := make([]string, 0)
	tempValue := make([]string, 0)
	newl := 0

	if len(m.next) > 0  {
		for k, b := range m.m {
			if m.next[k] != nil {
				cid = traversal(m.next[k])
				rootKeys = append(rootKeys, strconv.Itoa(int(k.(uint32))))
				result = append(result, cid)
				continue
			} else if m.m[k] != nil {
				newl = 0
				for k, v := range b.bucket {
					tempKey = append(tempKey, k.(string))
					tempValue = append(tempValue, v)
					newl += len(tempKey) + len(tempValue)
				}

				if l + newl > BlockSize {
					ul := &uploadBucket {
						Type: l,
						Key: tempKey[:len(tempKey) - len(b.bucket)],
						Value: tempValue[:len(tempValue) - len(b.bucket)],
					}
					in, _ := json.Marshal(ul)
					cid = ipfs.UploadIndex(string(in))
					rootKeys = append(rootKeys, templ0Key...)
					for len(templ0Key) > 0 {
						result = append(result, cid)
						templ0Key = templ0Key[:len(templ0Key) - 1]
					}
					tempKey = tempKey[len(tempKey) - len(b.bucket):]
					tempValue = tempValue[len(tempValue) - len(b.bucket):]
					l = newl
				} else {
					l += newl
				}

				templ0Key = append(templ0Key, strconv.Itoa(int(k.(uint32))))
			}
		}

		if len(tempKey) > 0 {
			ul := &uploadBucket {
				Type: l,
				Key: tempKey[:],
				Value: tempValue[:],
			}
			in, _ := json.Marshal(ul)
			cid = ipfs.UploadIndex(string(in))
			rootKeys = append(rootKeys, templ0Key...)
			for len(templ0Key) > 0 {
				result = append(result, cid)
				templ0Key = templ0Key[:len(templ0Key) - 1]
			}
		}

		cid = ""
		for len(rootKeys) > MaxKey {
			ul := &uploadBucket {
				Type: 1,
				Key: rootKeys[:MaxKey],
				Value: result[:MaxKey],
				Next: cid,
			}
			in, _ := json.Marshal(ul)
			cid = ipfs.UploadIndex(string(in))
			rootKeys = rootKeys[MaxKey:]
			result = result[MaxKey:]
		}

		ul := &uploadBucket {
			Type: 1,
			Key: rootKeys,
			Value: result,
			Next: cid,
		}
		in, _ := json.Marshal(ul)
		cid = ipfs.UploadIndex(string(in))

		return cid
	}

	l = 0
	for h, b := range m.m {
		newl = 0
		for k, v := range b.bucket {
			tempKey = append(tempKey, k.(string))
			tempValue = append(tempValue, v)
			newl += len(tempKey) + len(tempValue)
		}

		if l + newl > BlockSize {
			ul := &uploadBucket {
				Type: l,
				Key: tempKey[:len(tempKey) - len(b.bucket)],
				Value: tempValue[:len(tempValue) - len(b.bucket)],
			}
			in, _ := json.Marshal(ul)
			cid = ipfs.UploadIndex(string(in))
			l0Keys = append(l0Keys, templ0Key...)
			for len(templ0Key) > 0 {
				l0Cids = append(l0Cids, cid)
				templ0Key = templ0Key[:len(templ0Key) - 1]
			}
			tempKey = tempKey[len(tempKey) - len(b.bucket):]
			tempValue = tempValue[len(tempValue) - len(b.bucket):]
			l = newl
		} else {
			l += newl
		}

		templ0Key = append(templ0Key, strconv.Itoa(int(h.(uint32))))
	}

	if len(tempKey) > 0 {
		ul := &uploadBucket {
			Type: l,
			Key: tempKey[:],
			Value: tempValue[:],
		}
		in, _ := json.Marshal(ul)
		cid = ipfs.UploadIndex(string(in))
		l0Keys = append(l0Keys, templ0Key...)
		for len(templ0Key) > 0 {
			l0Cids = append(l0Cids, cid)
			templ0Key = templ0Key[:len(templ0Key) - 1]
		}
	}

	cid = ""
	for len(l0Keys) > MaxKey {
		ul := &uploadBucket {
			Type: 1,
			Key: l0Keys[:MaxKey],
			Value: l0Cids[:MaxKey],
			Next: cid,
		}
		in, _ := json.Marshal(ul)
		cid = ipfs.UploadIndex(string(in))
		l0Keys = l0Keys[MaxKey:]
		l0Cids = l0Cids[MaxKey:]
	}

	ul := &uploadBucket {
		Type: 1,
		Key: l0Keys,
		Value: l0Cids,
		Next: cid,
	}
	in, _ := json.Marshal(ul)
	cid = ipfs.UploadIndex(string(in))

	return cid
}

func bucketToNode(bucket *Bucket) string {
	keys := make([]string, 0)
	values := make([]string, 0)
	l := 0
	cid := ""
	for k, v := range bucket.bucket {
		if l+len(k.(string))+len(v) > BlockSize {
			ul := &uploadBucket{
				Type:  -1,
				Key:   keys,
				Value: values,
				Next:  cid,
			}
			in, _ := json.Marshal(ul)
			cid = ipfs.UploadIndex(string(in))
			keys = keys[0:0]
			values = values[0:0]
			l = 0
		}
		l += len(k.(string)) + len(v)
		keys = append(keys, k.(string))
		values = append(values, v)
	}

	ul := &uploadBucket{
		Type:  -1,
		Key:   keys,
		Value: values,
		Next:  cid,
	}
	in, _ := json.Marshal(ul)
	cid = ipfs.UploadIndex(string(in))

	return cid
}

func Update(cid string, words map[string]string) string {
	newCid := cid
	for k := range words {
		hash := Hash(k)
		newCid = update(k, newCid, hash, 0, words)
	}

	return newCid
}

func update(keyWord string, cid string, hash []uint32, level int, words map[string]string) string  {
	fmt.Println(keyWord, cid)
	value, pathList, levelList := Get(keyWord, cid, hash, level)
	content := ipfs.CatIndex(pathList[len(pathList) - 1])
	pathList = pathList[:len(pathList) - 1]
	level = levelList[len(levelList) - 1]
	levelList = levelList[:len(levelList) - 1]
	n := &uploadBucket{}
	err := json.Unmarshal([]byte(content), &n)

	if err != nil {
		log.Printf("uploadInner error : unmarshal error: %v", err)
	}
	switch value {
	case "":
		switch level {
		case 0:
			n.Key = append(n.Key, strconv.Itoa(int(hash[0])))
			data := &uploadBucket{
				Key: []string{keyWord},
				Value: []string{words[keyWord]},
				Type: -1,
				Next: "",
			}

			in, _ := json.Marshal(data)
			cid = ipfs.UploadIndex(string(in))
			n.Value = append(n.Value, cid)
			newCid := levelToCid(n)
			return newCid
		case 1, 2:
			n.Key = append(n.Key, strconv.Itoa(int(hash[level])))
			data := &uploadBucket{
				Key: []string{keyWord},
				Value: []string{words[keyWord]},
				Type: -1,
				Next: "",
			}

			in, _ := json.Marshal(data)
			cid = ipfs.UploadIndex(string(in))
			n.Value = append(n.Value, cid)
			newCid := levelToCid(n)
			newCid = pathToCid(pathList, levelList, newCid, level, keyWord, hash)
			return newCid

		case -1:
			n.Key = append(n.Key, keyWord)
			n.Value = append(n.Value, words[keyWord])

			bucket := &Bucket{}
			b := make(map[interface{}]string)
			for i, k := range n.Key {
				b[k] = n.Value[i]
			}
			bucket.bucket = b
			newCid := bucketToNode(bucket)
			newCid = pathToCid(pathList, levelList, newCid, level, keyWord, hash)
			return newCid
		}
	default:
		bucket := &Bucket{}
		b := make(map[interface{}]string)
		for i, k := range n.Key {
			if k == keyWord {
				n.Value[i] += n.Value[i] + words[k]
			}
			b[k] = n.Value[i]
		}
		bucket.bucket = b

		newCid := bucketToNode(bucket)

		newCid = pathToCid(pathList, levelList, newCid, level, keyWord, hash)
		return newCid

	}
	return ""
}

func levelToCid(n *uploadBucket) string {
	for len(n.Key) > MaxKey {
		ul := &uploadBucket {
			Type: 0,
			Key: n.Key[:MaxKey],
			Value: n.Value[:MaxKey],
			Next: n.Next,
		}
		in, _ := json.Marshal(ul)
		n.Next = ipfs.UploadIndex(string(in))
		n.Key = n.Key[MaxKey:]
		n.Value = n.Value[MaxKey:]
	}

	ul := &uploadBucket {
		Type: 0,
		Key: n.Key,
		Value: n.Value,
		Next: n.Next,
	}
	in, _ := json.Marshal(ul)
	cid := ipfs.UploadIndex(string(in))
	return cid
}

func pathToCid(pathList []string, levelList []int, newCid string, level int, keyWord string, hash []uint32) string {
	cur := -2
	for len(pathList) > 0 {
		if levelList[len(levelList) - 1] == -1 {
			cur -= 1
		}else {
			cur = levelList[len(levelList) - 1]
		}
		content := ipfs.CatIndex(pathList[len(pathList) - 1])
		pathList = pathList[:len(pathList) - 1]
		n := &uploadBucket{}
		err := json.Unmarshal([]byte(content), &n)

		if err != nil {
			log.Printf("pathToList error : unmarshal error: %v", err)
		}
		if level == levelList[len(levelList) - 1] {
			n.Next = newCid
		} else {
			for i, k := range n.Key {
				if k == strconv.Itoa(int(hash[cur])) {
					n.Value[i] = newCid
					break
				}
			}
		}

		level = levelList[len(levelList) - 1]
		levelList = levelList[:len(levelList) - 1]
		in, _ := json.Marshal(n)
		newCid = ipfs.UploadIndex(string(in))
	}

	return newCid
}

//func mergeVersion(cid1 string, cid2 string) string {
//
//}
