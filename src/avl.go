package lsm

import (
	"container/list"
	"fmt"
	"math"
	"strings"
	"unsafe"
)

// var d string = "-1"

type AVL interface {
	LeftTree() AVL
	RightTree() AVL
	RootKey() string
	Insert(string, []byte) AVL
	Print()
	Height() int
	Inorder(pairs *[]Pair)
	Search(string) *Pair
	Size() int
}

type Pair struct {
	Key  string
	Val  []byte
	Tomb bool
}

type AVLNode struct {
	entry  Pair
	Left   *AVLNode
	Right  *AVLNode
	Weight int
}

func (node *AVLNode) RootKey() string {
	if node == nil {
		return ""
	}
	return node.entry.Key
}

func (node *AVLNode) Size() (s int) {
	seg := NewSegment(node)
	s += int(unsafe.Sizeof(seg))
	for _, p := range seg.Pairs {
		s += int(unsafe.Sizeof(p))
	}
	return
}

func (node *AVLNode) LeftTree() AVL {
	if node == nil {
		return nil
	}
	return node.Left
}

func (node *AVLNode) RightTree() AVL {
	if node == nil {
		return nil
	}
	return node.Right
}

// val == nil, delete the element
func (node *AVLNode) Insert(key string, val []byte) AVL {
	if node == nil {
		if val == nil {
			// Cannot delete a node before it is created !
			fmt.Println("Cannot delete a node before it is created")
			return nil
		}
		return &AVLNode{entry: Pair{Key: key, Val: val, Tomb: false}, Weight: 0}
	}

	if strings.Compare(key, node.entry.Key) == -1 {
		node.Left, _ = node.LeftTree().Insert(key, val).(*AVLNode)
	} else if strings.Compare(key, node.entry.Key) == 1 {
		node.Right, _ = node.Right.Insert(key, val).(*AVLNode)
	} else {
		// Same Value
		// node.Key = key
		if val == nil {
			node.entry.Tomb = true
		} else {
			node.entry.Val = val
		}
		return node
	}

	node.Weight = node.Left.Height() - node.Right.Height()

	return node.Balanced()
}

func (node *AVLNode) Search(key string) *Pair {
	if node == nil {
		return nil
	}
	if strings.Compare(key, node.entry.Key) == -1 {
		return node.LeftTree().Search(key)
	} else if strings.Compare(key, node.entry.Key) == 1 {
		return node.RightTree().Search(key)
	} else {
		return &node.entry
	}
}

func (node *AVLNode) Balanced() *AVLNode {
	if node == nil {
		return node
	}
	if node.Weight <= 1 && node.Weight >= -1 {
		// fmt.Println("[BALANCED]")
		return node
	}

	if node.Left != nil && node.Weight > 1 && node.Left.Weight > 1 {
		// fmt.Println("[balancing]: left left case")

		temp := node.Left
		node.Left = node.Left.Right
		temp.Right = node
		node = temp
	} else if node.Left != nil && node.Weight > 1 && node.Left.Weight < 1 {
		// fmt.Println("[balancing]: left right case")

		temp := node.Left.Right
		node.Left.Right = node.Left.Right.Left
		temp.Left = node.Left
		node.Left = temp

		temp = node.Left
		node.Left = node.Left.Right
		temp.Right = node
		node = temp

	} else if node.Right != nil && node.Weight < 1 && node.Right.Weight > 1 {
		// fmt.Println("[balancing]: right left case")

		temp := node.Right.Left
		node.Right.Left = node.Right.Left.Right
		temp.Right = node.Right
		node.Right = temp

		temp = node.Right
		node.Right = node.Right.Left
		temp.Left = node
		node = temp

	} else if node.Right != nil && node.Weight < 1 && node.Right.Weight < 1 {
		// fmt.Println("[balancing]: right right case")

		temp := node.Right
		node.Right = node.Right.Left
		temp.Left = node
		node = temp
	}

	return node
}

func (node *AVLNode) Inorder(pairs *[]Pair) {
	if node == nil {
		return
	}
	if pairs == nil {
		pairs = &[]Pair{}
	}
	node.Left.Inorder(pairs)
	*pairs = append(*pairs, node.entry)
	node.Right.Inorder(pairs)
}

func (node *AVLNode) Print() {
	if node == nil {
		return
	}

	queue := list.New()

	queue.PushBack(node)

	for queue.Len() != 0 {
		n := queue.Len()
		for i := 0; i < n; i++ {
			front := queue.Front()
			val, _ := front.Value.(*AVLNode)
			//change to val
			fmt.Print(val.entry.Key, " ")
			queue.Remove(front)
			if val.Left != nil {
				queue.PushBack(val.Left)
			}
			// else if *val.Key != "-1" {
			// 	queue.PushBack(&AVLNode{Key: &d})
			// }
			if val.Right != nil {
				queue.PushBack(val.Right)
			}
			//  else if *val.Key != "-1" {
			// 	queue.PushBack(&AVLNode{Key: &d})
			// }
		}
		fmt.Print("\n")
	}

}

func (node *AVLNode) Height() int {
	if node == nil {
		return 0
	}
	return 1 + max(node.Left.Height(), node.Right.Height())
}

func InitAVLTree() *AVLNode {
	return nil
}

func Validate(tree AVL) bool {
	if tree == nil {
		return true
	}
	left := 0
	right := 0
	if tree.LeftTree() != nil {
		left = tree.LeftTree().Height()
	}
	if tree.RightTree() != nil {
		right = tree.RightTree().Height()
	}
	// fmt.Printf("Left: %v, Right: %v, Key: %v \n", left, right, tree.RootKey())
	if math.Abs(float64(left-right)) > 1 {
		return false
	}
	return Validate(tree.LeftTree()) && Validate(tree.RightTree())
}
