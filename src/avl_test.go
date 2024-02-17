package lsm

import (
	"fmt"
	"testing"
)

func TestAVLTree(t *testing.T) {

	keys := []string{"13", "10", "15", "5", "11", "16", "4", "6", "14"}

	for i := range 100000 {
		keys = append(keys, fmt.Sprint(i))
	}

	var tree AVL = InitAVLTree()

	for _, key := range keys {
		tree = tree.Insert(key, []byte(key))
	}

	if Validate(tree) == false {
		t.Fatal("The tree is not AVL tree")
	}
}
