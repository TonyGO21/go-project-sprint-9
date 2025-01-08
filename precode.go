package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Generator генерирует последовательность чисел 1,2,3 и т.д.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	defer close(ch)
	var i int64 = 1
	for {
		select {
		case <-ctx.Done():
			return
		case ch <- i:
			fn(i)
			i++
		}
	}
}

// Worker читает число из канала in и пишет его в канал out.
func Worker(ctx context.Context, in <-chan int64, out chan<- int64) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return
		case num, ok := <-in:
			if !ok {
				return
			}
			out <- num
		}
	}
}

func main() {
	chIn := make(chan int64)

	// Создание контекста с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var inputSum int64
	var inputCount int64

	// Генератор чисел
	go Generator(ctx, chIn, func(i int64) {
		inputSum += i
		inputCount++
	})

	const NumOut = 5
	outs := make([]chan int64, NumOut)
	for i := 0; i < NumOut; i++ {
		outs[i] = make(chan int64)
		go Worker(ctx, chIn, outs[i])
	}

	// Собираем числа из каналов outs
	amounts := make([]int64, NumOut)
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup
	for i := 0; i < NumOut; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for num := range outs[i] {
				chOut <- num
				amounts[i]++
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(chOut)
	}()

	var count int64
	var sum int64

	// Чтение данных из результирующего канала
	for num := range chOut {
		sum += num
		count++
	}

	fmt.Println("Количество чисел", inputCount, count)
	fmt.Println("Сумма чисел", inputSum, sum)
	fmt.Println("Разбивка по каналам", amounts)

	if inputSum != sum {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", inputSum, sum)
	}
	if inputCount != count {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", inputCount, count)
	}
	for _, v := range amounts {
		inputCount -= v
	}
	if inputCount != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное\n")
	}
}
