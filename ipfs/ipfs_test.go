package ipfs

import (
	"encoding/json"
	"fmt"
	"testing"
	"zly.ecnu.edu.cn/go-bptree-ipld/src/bptree"
)

func TestGet(t *testing.T)  {
	str := CatIndex("Qmdgbtys4RupFxXv1pHPBWB8tGqiXYUtnarHaZXXm3dSNQ")
	node := bptree.LeafNode{}

	_ := json.Unmarshal([]byte(str), &node)
	fmt.Println(len(node.Terms))

}
