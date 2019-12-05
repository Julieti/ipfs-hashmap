package hashmap

import (
	"encoding/json"
	"github.com/spaolacci/murmur3"
	"log"
	"strconv"
	"strings"
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

type word2Hash struct {
	hash []uint32
}

type VersionNote struct {
	LastVersion string `json:"last_version"`
	Root string `json:"root"`
	Date string `json:"date"`
	DocumentCount int `json:"document_count"`
	WordCount int `json:"word_count"`
	BloomFilter string `json:"bloom_filter"`
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
	hash := murmur3.Sum32([]byte(key.(string)))
	//hash := GetHash([]byte(key.(string)))
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

				for n.Next != "" {
					content := ipfs.CatIndex(n.Next)
					newCid := n.Next
					err = json.Unmarshal([]byte(content), &n)
					for j, key := range n.Key {
						if key == keyWord {
							return n.Value[j], newCid, true
						}
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

func Update(cid string, words map[string]string, w2H map[string]*word2Hash, keys map[string]string) string {
	n := &uploadBucket{}
	content := ipfs.CatIndex(cid)
	err := json.Unmarshal([]byte(content), &n)

	if err != nil {
		log.Printf("uploadInner error : unmarshal error: %v", err)
	}


	newCid := updateANode(n, 0, words, keys, w2H)


	return newCid
}

func updateANode(n *uploadBucket, level int, words map[string]string, keys map[string]string, w2H map[string]*word2Hash) string {
	processed := make(map[string]string)
	pos := make(map[int]string, 0)
	for i := 0; i < len(n.Key); i++ {
		for k := range keys{
			if strconv.Itoa(int(w2H[k].hash[level])) == n.Key[i] {
				pos[i] = n.Key[i]
				if _,ok := processed[n.Key[i]]; ok {
					processed[n.Key[i]] = processed[n.Key[i]] + " " + k
				} else {
					processed[n.Key[i]] = k
				}
			}
		}
	}
	data := &uploadBucket{}
	for k, v := range pos{
		if len(processed) == 0 {
			break
		}

		if _,ok := processed[v]; ok {
			content := ipfs.CatIndex(n.Value[k])
			ccid := n.Value[k]
			err := json.Unmarshal([]byte(content), &data)

			if err != nil {
				log.Printf("uploadInner error : unmarshal error: %v", err)
			}

			newl := 0
			nokeyl := 0
			count := 0
			nK := make([]string, 0)
			nV := make([]string, 0)
			lK := make([]string, 0)
			lV := make([]string, 0)
			if data.Type > 1 && n.Next == ""{ // 叶节点
				now := -1
				for l := 0; l < len(data.Key); l++ {
					if strconv.Itoa(int(Hash(data.Key[l])[level])) == v {
						now = l
						break
					}
				}

				for ll := now; ll < len(data.Key); ll ++ {
					if strconv.Itoa(int(Hash(data.Key[ll])[level])) == v {
						nK = append(nK, data.Key[ll])
						nV = append(nV, data.Value[ll])
						nokeyl += len(data.Key[ll]) + len(data.Value[ll])
						newl += len(data.Key[ll]) + len(data.Value[ll])
						count ++ //原data里key的数量
					} else {
						break
					}
				}

				ks := strings.Split(processed[v], " ")
				nokeyl, ks, nV := mergeSameKeys(nK, nV, ks, words, nokeyl)

				cid := ""
				if count + len(ks) > MaxBucketEntry {
					data.Type = data.Type - newl
					d := &uploadBucket{}
					for _, k := range ks {
						nK = append(nK, k)
						nV = append(nV, words[k])
						nokeyl += len(k) + len(words[k])

						if nokeyl > BlockSize {
							d.Key = nK[:len(nK)- 1]
							d.Value = nV[:len(nV)-1]
							d.Type = nokeyl - len(k) - len(words[k])
							d.Next = cid
							in, _ := json.Marshal(d)
							cid = ipfs.UploadIndex(string(in))

							nK = nK[len(nK)-1:]
							nV = nV[len(nV)-1:]
							nokeyl = len(k) + len(words[k])
						}
					}

					d.Key = nK[:]
					d.Value = nV[:]
					d.Type = nokeyl
					d.Next = cid

					in, _ := json.Marshal(d)
					cid = ipfs.UploadIndex(string(in))
					n.Value[k] = cid

					data.Key = append(data.Key[:now], data.Key[now+count :]...)
					data.Value = append(data.Value[:now], data.Value[now+count :]...)
				} else {
					nl := 0
					for _, k := range ks {
						lK = append(lK, k)
						lV = append(lV, words[k])
						nl  +=  len(k) + len(words[k])
					}

					if data.Type + nl > BlockSize {
						d := &uploadBucket{}
						d.Key = append(nK, lK...)
						d.Value = append(nV, lV...)
						d.Type = nl + nokeyl
						in, _ := json.Marshal(d)
						cid = ipfs.UploadIndex(string(in))
						n.Value[k] = cid

						data.Key = append(data.Key[:now], data.Key[now+count:]...)
						data.Value = append(data.Value[:now], data.Value[now+count:]...)
						data.Type = data.Type - newl
					} else {
						a := data.Key[now+count:]
						a = append(lK, a...)
						b := data.Value[now+count:]

						data.Key = append(data.Key[:now], nK...)
						data.Key = append(data.Key, a...)

						data.Value = append(data.Value[:now], nV...)
						data.Value = append(data.Value, lV...)
						data.Value = append(data.Value, b... )
						data.Type = data.Type - newl + nokeyl + nl
					}
				}

				in, _ := json.Marshal(data)
				cid2 := ipfs.UploadIndex(string(in))

				for i := 0; i < len(n.Key); i++  {
					 if n.Value[i] == ccid {
						n.Value[i] = cid2
					}
				}
			} else if data.Type > 1 && n.Next != "" {
				cid := data.Next
				ks := strings.Split(processed[v], " ")

				nokeyl, ks, data.Value = mergeSameKeys(data.Key, data.Value, ks, words, data.Type)
				data.Type = nokeyl


				for _, k := range ks {
					data.Key = append(data.Key, k)
					data.Value = append(data.Value, words[k])
					newl += len(k) + len(words[k])

					if data.Type + newl > BlockSize {
						data.Key = data.Key[:len(data.Key)- 1]
						data.Value = data.Value[:len(data.Value)-1]
						data.Next = cid
						in, _ := json.Marshal(data)
						cid = ipfs.UploadIndex(string(in))

						data.Key = data.Key[len(data.Key)-1:]
						data.Value = data.Value[len(data.Value)-1:]
						data.Type = len(k) + len(words[k])
						newl = len(k) + len(words[k])
					} else {
						data.Type += newl
					}
				}

				data.Next = cid
				in, _ := json.Marshal(data)
				cid = ipfs.UploadIndex(string(in))
				n.Value[k] = cid
			} else {
				ks := strings.Split(processed[v], " ")
				nk := make(map[string]string)
				for _, k := range ks {
					nk[k] = ""
				}

				cid := updateANode(data, level + 1, words, nk, w2H)
				n.Value[k] = cid
			}

			kkk := strings.Split(processed[v], " ")
			for _, k := range kkk{
				delete(keys, k)
			}
			delete(processed, v)
		}
	}

	data = &uploadBucket{}
	newProcessed := make(map[string]string, 0)
	for k := range keys {
		if _, ok := newProcessed[strconv.Itoa(int(w2H[k].hash[level]))]; ok {
			newProcessed[strconv.Itoa(int(w2H[k].hash[level]))] = newProcessed[strconv.Itoa(int(w2H[k].hash[level]))] + " "+ k
		} else {
			newProcessed[strconv.Itoa(int(w2H[k].hash[level]))] = k
		}
	}

	for k, v := range newProcessed {
		newl := 0
		n.Key = append(n.Key, k)
		keys := strings.Split(v, " ")

		for _, kk := range keys {
			data.Key = append(data.Key, kk)
			data.Value = append(data.Value, words[kk])
			newl += len(kk) + len(words[kk])
		}


		if data.Type + newl > BlockSize {
			data.Key = data.Key[: len(data.Key) - len(keys)]
			data.Value = data.Value[: len(data.Value) - len(keys)]
			in, _ := json.Marshal(data)
			cid := ipfs.UploadIndex(string(in))

			for i := len(n.Key) - len(n.Value) - 1; i > 0; i-- {
				n.Value = append(n.Value, cid)
			}
			data.Key = data.Key[len(data.Key) - len(keys) :]
			data.Value = data.Value[len(data.Value) - len(keys) :]
			data.Type = newl
		} else {
			data.Type += newl
		}
	}

	in, _ := json.Marshal(data)
	cid := ipfs.UploadIndex(string(in))

	for i := len(n.Key) - len(n.Value); i > 0; i-- {
		n.Value = append(n.Value, cid)
	}

	in, _ = json.Marshal(n)
	return ipfs.UploadIndex(string(in))
}

func mergeSameKeys(nK []string, nV []string, ks []string, words map[string]string, nokeyl int) (int, []string, []string) {
	for i := 0; i < len(nK); i++ {
		nokeyl += len(nK[i])
		for j := 0; j < len(ks); j++ {
			if nK[i] == ks[j] {
				nokeyl += len(words[ks[j]])
				nV[i] = nV[i] + words[ks[j]]
				ks = append(ks[:j], ks[j+1:]...)
				break
			}
		}

	}


	return  nokeyl, ks, nV
}

func VersionMerge(cid1 string, cid2 string, level int) string {
	levelKeys := make(map[string]string)
	l1 := &uploadBucket{}
	content := ipfs.CatIndex(cid1)
	err := json.Unmarshal([]byte(content), &l1)

	//less term
	l2 := &uploadBucket{}
	content = ipfs.CatIndex(cid2)
	err = json.Unmarshal([]byte(content), &l2)

	if err != nil {
		log.Printf("version merge error: unmarshal error: %v", err)
	}
	
	for i, k2 := range l2.Key {
		levelKeys[k2] = l2.Value[i]
	}
	
	for j, k1 := range l1.Key {
		words := make(map[string]string)
		w := make(map[string]*word2Hash)
		keys := make(map[string]string, 0)
		if _, ok := levelKeys[k1]; ok {
			l11 := &uploadBucket{}
			content = ipfs.CatIndex(l1.Value[j])
			err = json.Unmarshal([]byte(content), &l11)

			l22 := &uploadBucket{}
			content := ipfs.CatIndex(levelKeys[k1])
			err = json.Unmarshal([]byte(content), &l22)
			
			if l11.Type > 1 && l22.Type > 1 {
				cid := mergeTwoBucket(l11, l22)
				l1.Value[j] = cid
				
			} else if l11.Type > 1 {
				for i , kk := range l11.Key {
					words[kk] = l11.Value[i]
					keys[kk] = ""
					h := Hash(kk)
					w2H := &word2Hash{
						hash: h,
					}
					w[kk] = w2H
				}
				cid := updateANode(l22, level+1,  words, keys, w)
				l1.Value[j] = cid
			} else if l22.Type > 1 {
				for i , kk := range l22.Key {
					words[kk] = l22.Value[i]
					keys[kk] = ""
					h := Hash(kk)
					w2H := &word2Hash{
						hash: h,
					}
					w[kk] = w2H
				}
				cid := updateANode(l11, level + 1, words, keys, w)
				l1.Value[j] = cid
			} else {
				cid := VersionMerge(l1.Value[j], levelKeys[k1], level + 1)
				l1.Value[j] = cid
			}
			delete(levelKeys, k1)
		}
	}


	for k, v := range levelKeys {
		l1.Key = append(l1.Key, k)
		l1.Value = append(l1.Value, v)
	}

	in, _ := json.Marshal(l1)
	return ipfs.UploadIndex(string(in))
}

func mergeTwoBucket(l11 *uploadBucket, l22 *uploadBucket) string {
	newKV := make(map[string]string)
	newUp := &uploadBucket{}

	for i, k := range l11.Key {
		newKV[k] = l11.Value[i]
	}

	for l11.Next != "" {
		content := ipfs.CatIndex(l11.Next)
		err := json.Unmarshal([]byte(content), &l11)

		if err != nil {
			log.Printf("merge bucket error: unmarshal error: %v", err)
		}

		for i, k := range l11.Key {
			newKV[k] = newKV[k] + l11.Value[i]
		}
	}

	for i, k := range l22.Key {
		if _, ok := newKV[k]; ok {
			newKV[k] = newKV[k] + l22.Value[i]
		} else {
			newKV[k] = l22.Value[i]
		}
	}

	for l22.Next != "" {
		content := ipfs.CatIndex(l22.Next)
		err := json.Unmarshal([]byte(content), &l22)

		if err != nil {
			log.Printf("merge bucket error: unmarshal error: %v", err)
		}

		for i, k := range l22.Key {
			if _, ok := newKV[k]; ok {
				newKV[k] = newKV[k] + l22.Value[i]
			} else {
				newKV[k] = l22.Value[i]
			}
		}
	}

	next := ""
	for k, v := range newKV {
		newUp.Key = append(newUp.Key, k)
		newUp.Value = append(newUp.Value, v)
		newUp.Type += len(k) + len(v)

		if newUp.Type > BlockSize {
			newUp.Key = newUp.Key[: len(newUp.Key) - 1]
			newUp.Value = newUp.Value[: len(newUp.Value) - 1]
			newUp.Type = newUp.Type - len(k) - len(v)
			newUp.Next = next
			in, _ := json.Marshal(newUp)
			next = ipfs.UploadIndex(string(in))

			newUp.Key = newUp.Key[len(newUp.Key) - 1:]
			newUp.Value = newUp.Value[len(newUp.Value) - 1:]
			newUp.Type = len(k) + len(v)
		}
	}

	in, _ := json.Marshal(newUp)
	next = ipfs.UploadIndex(string(in))

	return next
}

