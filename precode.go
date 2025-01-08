package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

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

func Worker(in <-chan int64, out chan<- int64) {
	defer close(out)
	for num := range in {
		out <- num
	}
}

func main() {
	chIn := make(chan int64)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var inputSum int64
	var inputCount int64
	var mu sync.Mutex // Мьют для синхронизации

	// Генератор чисел
	go Generator(ctx, chIn, func(i int64) {
		mu.Lock()     // Блокировка мью
		inputSum += i // Увеличение inputSum
		inputCount++  // Увеличение inputCount
		mu.Unlock()   // Освобождение мью
	})

	const NumOut = 5
	outs := make([]chan int64, NumOut)
	for i := 0; i < NumOut; i++ {
		outs[i] = make(chan int64)
		go Worker(chIn, outs[i])
	}

	amounts := make([]int64, NumOut)
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup
	for i := 0; i < NumOut; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for num := range outs[i] {
				chOut <- num
				amounts[i]++ // Увеличение разбивки по каналам
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(chOut)
	}()

	var count int64
	var sum int64

	for num := range chOut {
		sum += num
		count++
	}

	mu.Lock() // Блокировка мью перед чтением
	fmt.Println("Количество чисел", inputCount, count)
	fmt.Println("Сумма чисел", inputSum, sum)
	fmt.Println("Разбивка по каналам", amounts)
	mu.Unlock() // Освобождение мью после чтения

	if inputSum != sum {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", inputSum, sum)
	}
	if inputCount != count {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", inputCount, count)
	}

	for _, v := range amounts {
		mu.Lock() // Блокировка мью для изменения
		inputCount -= v
		mu.Unlock() // Освобождение мью

	}

	mu.Lock() // Блокировка мью перед финальной проверкой
	if inputCount != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное\n")
	}
	mu.Unlock() // Освобождение мью
}
