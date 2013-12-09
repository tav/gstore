package memdb

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
)

func TestMemDB(t *testing.T) {
	memDB := New()
	if memDB.Empty() == false {
		log.Fatal("memDB should be empty")
	}
	fmt.Printf("Memory Usage After Compaction: %v bytes\n", memDB.ApproximateMemoryUsage())
	var val []byte
	var err error
	memDB.Set([]byte("hello"), []byte("world"))
	val, err = memDB.Get([]byte("hello"))
	if err != nil {
		panic(err)
		log.Fatal(err)
		fmt.Println(err)
	}
	fmt.Println(string(val))
	memDB.Delete([]byte("hello"))
	var intVal int
	for x := 1; x < 200000; x++ {
		memDB.Set([]byte(strconv.Itoa(x)), []byte(strconv.Itoa(x+1)))
		//fmt.Printf("Put Val %v\n", strconv.Itoa(x+1))
		val, err = memDB.Get([]byte(strconv.Itoa(x)))
		intVal, err = strconv.Atoi(string(val))
		if x+1 != intVal {
			log.Fatal("incorrect return value")
		}
		//fmt.Printf("Got Val %v\n", intVal)
	}
	fmt.Printf("Memory Usage: %v bytes\n", memDB.ApproximateMemoryUsage())
	memDB.Delete([]byte("150"))
	val, err = memDB.Get([]byte("150"))
	if err == ErrNotFound {
		fmt.Printf("%v\n", err)
	} else {
		log.Fatal("Deleted Value is still present")
	}
	if memDB.Empty() == true {
		log.Fatal("memDB thinks it is empty")
	}
	for x := 1; x < 200000; x++ {
		memDB.Set([]byte(strconv.Itoa(x)), []byte(strconv.Itoa(x+2)))
		val, err = memDB.Get([]byte(strconv.Itoa(x)))
		intVal, err = strconv.Atoi(string(val))
		if x+2 != intVal {
			log.Fatal("incorrect return value")
		}
		//fmt.Printf("%v\n", intVal)
	}
	fmt.Printf("Memory Usage: %v bytes\n", memDB.ApproximateMemoryUsage())
	newMemDB := Compact(memDB)
	fmt.Printf("Memory Usage After Compaction: %v bytes\n", newMemDB.ApproximateMemoryUsage())
	if newMemDB.ApproximateMemoryUsage() >= memDB.ApproximateMemoryUsage() {
		log.Fatal("Compaction not working")
	}
	mem_file, err := os.Create("mem.amp") // For read access.
	if err != nil {
		log.Fatal(err)
	}
	Save(newMemDB, mem_file)
	if err := mem_file.Close(); err != nil {
		panic(err)
	}

	for x := 1; x < 200000; x++ {
		newMemDB.Delete([]byte(strconv.Itoa(x)))
	}
	if newMemDB.Dirty == false {
		log.Fatal("should be dirty")
	}
	newMemDB = Compact(newMemDB)
	if newMemDB.Dirty == true {
		log.Fatal("should not be dirty")
	}
	newMemDB.Empty()
	fmt.Printf("Memory Usage After Compaction: %v bytes\n", newMemDB.ApproximateMemoryUsage())
	fmt.Printf("node data: %v \n", newMemDB.nodeData)

	iter := newMemDB.Find([]byte{})
	for iter.Next() {
		fmt.Printf("iter.Key %v\n", string(iter.Key()))
		fmt.Printf("iter.Val %v\n", string(iter.Value()))
	}

	mem_file, err = os.Open("mem.amp")
	if err != nil {
		log.Fatal(err)
	}
	// close fi on exit and check for its returned error
	defer func() {
		if err := mem_file.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	memDB, err = Load(mem_file)
	fmt.Printf("Memory Usage: %v bytes\n", memDB.ApproximateMemoryUsage())
	val, err = memDB.Get([]byte(strconv.Itoa(2)))
	intVal, err = strconv.Atoi(string(val))
	fmt.Printf("%v\n", intVal)
	if intVal != 4 {
		log.Fatal("loading incorrectly")
	}
}

func BenchmarkMemDB(b *testing.B) {
	memDB := New()
	var val []byte
	var err error
	for x := 1; x < 2000000; x++ {
		memDB.Set([]byte(strconv.Itoa(x)), []byte(strconv.Itoa(x+1)))
		val, err = memDB.Get([]byte(strconv.Itoa(x)))
	}
	_ = val
	_ = err
}
func BenchmarkMap(b *testing.B) {
	memmap := map[string]string{}
	var valstr string
	for x := 1; x < 2000000; x++ {
		memmap[strconv.Itoa(x)] = strconv.Itoa(x + 1)
		valstr = memmap[strconv.Itoa(x)]
	}
	_ = valstr
}
