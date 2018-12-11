package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/Ian-MacLeod/database/btree"
)

func main() {
	tree := btree.New(3)

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	values := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	for i := 0; i < 10; i++ {
		tree.Upsert(keys[i], values[i])
	}

	for i := 0; i < 10; i++ {
		fmt.Println(tree.Find(keys[i]))
	}
	fmt.Println(tree.Find("ea"))
	fmt.Println(tree.Find("da"))
	fmt.Println(tree.Find(""))
	fmt.Println(tree.Find("k"))

	tree = btree.New(12)
	const numEntries = int(1e6)
	const numQueues = 8
	const queueSize = numEntries / numQueues
	var entries [][2]string
	var wg sync.WaitGroup

	r := rand.New(rand.NewSource(time.Now().Unix()))
	for _, i := range r.Perm(numEntries) {
		entry := [2]string{"key-" + strconv.Itoa(i), "value-" + strconv.Itoa(i)}
		entries = append(entries, entry)
	}

	var queues [][][2]string
	for queueNum := 0; queueNum < numQueues; queueNum++ {
		var queue [][2]string
		for i := queueNum * queueSize; i < (queueNum+1)*queueSize; i++ {
			queue = append(queue, entries[i])
		}
		queues = append(queues, queue)
	}

	addEntries := func(entries [][2]string) {
		for _, entry := range entries {
			tree.Upsert(entry[0], entry[1])
		}
		wg.Done()
	}
	start := time.Now()
	for _, queue := range queues {
		wg.Add(1)
		go addEntries(queue)
	}
	wg.Wait()
	end := time.Now()
	count := 0
	for _, entry := range entries {
		value, _ := tree.Find(entry[0])
		if value != entry[1] {
			fmt.Println("Not found: ")
			fmt.Println(entry)
			count++
		}
	}
	fmt.Println(count)
	fmt.Println(end.Sub(start))

	manager := tree.GetReadTransactionManager()
	for i := 0; i < numEntries; i++ {
		key, _ := manager.Find("key-" + strconv.Itoa(i))
		if key != "value-"+strconv.Itoa(i) {
			panic("Problem")
		}
	}
	fmt.Println(len(manager.Locks))
	manager.End()

	fuzz := func() {
		fuzz := time.Duration(rand.Int()%1000) * time.Millisecond
		time.Sleep(fuzz)
	}

	t1 := func() {
		fuzz()
		writeManager := tree.GetWriteTransactionManager()
		defer writeManager.End()
		fuzz()
		writeManager.Upsert("1", "a")
		fuzz()
		writeManager.Upsert("2", "a")
		fuzz()
		one, _ := writeManager.Find("1")
		fuzz()
		two, _ := writeManager.Find("2")
		if one != two {
			panic("Problem")
		}
		wg.Done()
	}

	t2 := func() {
		fuzz()
		writeManager := tree.GetWriteTransactionManager()
		defer writeManager.End()
		fuzz()
		writeManager.Upsert("1", "b")
		fuzz()
		writeManager.Upsert("2", "b")
		fuzz()
		one, _ := writeManager.Find("1")
		fuzz()
		two, _ := writeManager.Find("2")
		if one != two {
			panic("Problem")
		}
		wg.Done()
	}

	for i := 0; i < 10; i++ {
		wg.Add(2)
		go t1()
		go t2()
	}
	wg.Wait()

	// w1 := func() {
	// 	fuzz()
	// 	tree.Upsert("1", "a")
	// 	fuzz()
	// 	tree.Upsert("2", "a")
	// 	fuzz()
	// 	one, _ := tree.Find("1")
	// 	fuzz()
	// 	two, _ := tree.Find("2")
	// 	fuzz()
	// 	if one != two {
	// 		panic("Problem")
	// 	}
	// 	wg.Done()
	// }

	// w2 := func() {
	// 	fuzz()
	// 	tree.Upsert("1", "b")
	// 	fuzz()
	// 	tree.Upsert("2", "b")
	// 	fuzz()
	// 	one, _ := tree.Find("1")
	// 	fuzz()
	// 	two, _ := tree.Find("2")
	// 	fuzz()
	// 	if one != two {
	// 		panic("Problem")
	// 	}
	// 	wg.Done()
	// }

	// for i := 0; i < 10; i++ {
	// 	wg.Add(2)
	// 	go w1()
	// 	go w2()
	// }
	// wg.Wait()
}
