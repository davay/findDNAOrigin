package main

import (
	"bufio"
	"math"
	"os"
	"strings"
	"sync"
)

const NumThreads = 16
const ParallelLevels = 4
const Filename = "genome"

type TallyType struct {
	c int
	g int
}

func parent(i int) int {
	return (i - 1) / 2
}

func left(i int) int {
	return i*2 + 1
}

func right(i int) int {
	return left(i) + 1
}

func isLeaf(i int, size int) bool {
	return right(i) >= size*2-1
}

func lowestSkewPosition(skewMap []float32) int {
	var min float32 = math.MaxInt64
	index := -1
	for i := range skewMap {
		if skewMap[i] < min {
			min = skewMap[i]
			index = i
		}
	}
	return index
}
func startSum(output []TallyType, size int) {

	sig := make(chan int)
	go calcSum(0, 0, output, size, sig)
	<-sig
}

func calcSum(i int, level int, output []TallyType, size int, signal chan<- int) {
	//println("Starting ", i)

	if !isLeaf(i, size) {
		if level < ParallelLevels-1 {
			sig := make(chan int)
			sig2 := make(chan int)
			//var wg2 sync.WaitGroup
			go calcSum(left(i), level+1, output, size, sig)
			calcSum(right(i), level+1, output, size, sig2)
			<-sig
		} else {
			sig := make(chan int)
			sig2 := make(chan int)
			calcSum(left(i), level+1, output, size, sig)
			calcSum(right(i), level+1, output, size, sig2)
		}

		output[i].c = output[left(i)].c + output[right(i)].c
		output[i].g = output[left(i)].g + output[right(i)].g
	}
	//println("Done with ", i)
	close(signal)
}

func startSkew(input []TallyType, output []int, size int) {

	sig := make(chan int)
	x := TallyType{0, 0}
	go calcSkew(0, x, 0, input, output, size, sig)
	<-sig
}

func calcSkew(i int, sumPrior TallyType, level int, input []TallyType, output []int, size int, signal chan<- int) {
	if isLeaf(i, size) {

		output[i-size+1] = (sumPrior.g + input[i].g) - (sumPrior.c + input[i].c)
	} else {
		if level < ParallelLevels-1 {
			sig := make(chan int)
			sig2 := make(chan int)
			go calcSkew(left(i), sumPrior, level+1, input, output, size, sig)
			preSumPrior := TallyType{sumPrior.c + input[left(i)].c, sumPrior.g + input[left(i)].g}
			calcSkew(right(i), preSumPrior, level+1, input, output, size, sig2)
			<-sig
		} else {
			sig := make(chan int)
			sig2 := make(chan int)
			calcSkew(left(i), sumPrior, level+1, input, output, size, sig)
			preSumPrior := TallyType{sumPrior.c + input[left(i)].c, sumPrior.g + input[left(i)].g}
			calcSkew(right(i), preSumPrior, level+1, input, output, size, sig2)
		}
	}
	close(signal)
}
func mapSkew(output []TallyType, xCount int) []float32 {
	skew := make([]float32, len(output))
	for i := 0; i < len(output)-xCount; i++ {
		cPg := output[i].c + output[i].g
		cMg := output[i].c - output[i].g
		skew[i] = float32(cMg) / float32(cPg)
	}
	return skew
}

func findNextPowerTwo(input int) int {
	input--
	input |= input >> 1
	input |= input >> 2
	input |= input >> 4
	input |= input >> 8
	input |= input >> 16
	input++
	return input
}

func fixInput(input string) string {
	inputSize := len(input)
	paddingSize := findNextPowerTwo(inputSize) - inputSize
	println("PADDING SIZE: ", paddingSize)
	var padding strings.Builder
	padding.WriteString(input)
	for i := 0; i < paddingSize; i++ {
		padding.WriteString("X")
	}
	return padding.String()
}
func printData(input []TallyType, size int) {
	for i := 0; i < size; i++ {
		print(i, ": ")
		println("[", input[i].c, ",", input[i].g, "]")

	}
}

func parseInput(id int, wg *sync.WaitGroup, input string, data []TallyType, size int) {
	defer wg.Done()
	println("STARTING THREAD: ", id, " SIZE: ", size)
	for pos, char := range input[id*size : id*size+size] {
		y := TallyType{0, 0}
		if char == 'C' {
			y.c = 1
		}
		if char == 'G' {
			y.g = 1
		}
		data[pos+id*size] = y
	}

}

func main() {
	var wg sync.WaitGroup

	file, err := os.Open(Filename)
	if err != nil {
		//handle error
		return
	}
	defer file.Close()
	var input string
	s := bufio.NewScanner(file)
	for s.Scan() {
		input += s.Text()
	}
	input = fixInput(input)
	size := len(input)
	data := make([]TallyType, size*2-1)
	for i := 0; i < NumThreads; i++ {
		wg.Add(1)
		go parseInput(i, &wg, input, data, size/NumThreads)
	}
	wg.Wait()

	outputArr := make([]int, size)

	startSum(data, size)
	//calcSum(0, 0, data, size)
	println("done")

	startSkew(data, outputArr, size)

}
