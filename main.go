package main

import (
	"fmt"
	lsm "lsm/src"
)

// var (
// 	val string = "test"
// )

func main() {
	lsm, err := lsm.New()
	if err != nil {
		fmt.Println("Unable to initialise LSM tree, exiting")
		return
	}
	// go func() {
	// 	for {
	// 		if val := lsm.Search("45566"); val != nil {
	// 			fmt.Println(string(val) + "\n")
	// 		}
	// 	}
	// }()
	// var wg sync.WaitGroup

	for j := range 100 {
		// for j := 200; j < 300; j += 1 {
		for i := range 1000 {
			str := fmt.Sprintf("%v", j*1000+i)
			lsm.Insert(str, []byte(str))
			// tree = tree.Insert(str, []byte(str))
		}
	}

	lsm.Update("1000", []byte("1"))
	lsm.Delete("10000")

	fmt.Println("Searching....")
	// wg.Add(1)
	// go func() {
	// for {
	if val := lsm.Search("1000"); val != nil {
		fmt.Println(string(val))
		// wg.Done()
	}
	if val := lsm.Search("10000"); val != nil {
		fmt.Println(string(val))
		// wg.Done()
	}

	// time.Sleep(20 * time.Second)
	lsm.Exit()
	// }
	// }()
	// wg.Wait()

	// var tree src.AVL = src.InitAVLTree()

	// tree.Print()
	// fmt.Println(src.Validate(tree))
	// fmt.Println(lsm.String())
	// time.Sleep(time.Second)
}
