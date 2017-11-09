package main

import (
	"time"
	"fmt"
)

func main() {
	fmt.Println("run...")

	ch := make(chan int, 1)

	go func() {
		time.Sleep(time.Second * 2)

		ch <- 1

		time.Sleep(time.Second)

		ch <- 2
	}()

	fmt.Println(time.Now().Unix())

	for {
		var v int

		select {
		case v = <-ch:
			fmt.Println(v, time.Now().Unix())
		}

		if v == 2 {
			break
		}
	}
}


