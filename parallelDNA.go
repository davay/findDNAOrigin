package main

import (
	"bufio"
	"math"
	"os"
	"strings"
	"sync"
)

const NumThreads = 8
const ParallelLevels = 4
const Filename = "genome"

type MinSkew struct {
	index int
	value int
}

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

func lowestSkewPosition(skewMap []int, wg *sync.WaitGroup, start int, end int, mS *MinSkew) {
	defer wg.Done()
	min := math.MaxInt32
	index := -1
	for i := start; i < end; i++ {
		if skewMap[i] < min {
			min = skewMap[i]
			index = i
		}
	}
	mS.index = index
	mS.value = min
}
func startSum(output []TallyType, size int) {
	sig := make(chan int)
	calcSum(0, 0, output, size, sig)
}

func calcSum(i int, level int, output []TallyType, size int, signal chan<- int) {
	sig := make(chan int)
	sig2 := make(chan int)
	if !isLeaf(i, size) {
		if level < ParallelLevels-1 {
			go calcSum(left(i), level+1, output, size, sig)
		} else {
			calcSum(left(i), level+1, output, size, sig)
		}
		<-sig
		calcSum(right(i), level+1, output, size, sig2)
		<-sig2
		output[i].c = output[left(i)].c + output[right(i)].c
		output[i].g = output[left(i)].g + output[right(i)].g
	}
	close(signal)
}

func startSkew(input []TallyType, output []int, size int) {
	sig := make(chan int)
	x := TallyType{0, 0}
	calcSkew(0, x, 0, input, output, size, sig)
}

func calcSkew(i int, sumPrior TallyType, level int, input []TallyType, output []int, size int, signal chan<- int) {
	sig := make(chan int)
	sig2 := make(chan int)
	if isLeaf(i, size) {
		output[i-size+1] = (sumPrior.g + input[i].g) - (sumPrior.c + input[i].c)
	} else {
		if level < ParallelLevels-1 {
			go calcSkew(left(i), sumPrior, level+1, input, output, size, sig)
		} else {
			calcSkew(left(i), sumPrior, level+1, input, output, size, sig)
		}
		<-sig
		preSumPrior := TallyType{sumPrior.c + input[left(i)].c, sumPrior.g + input[left(i)].g}
		calcSkew(right(i), preSumPrior, level+1, input, output, size, sig2)
		<-sig2
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

func fixInput(input string) (string, int) {
	inputSize := len(input)
	paddingSize := findNextPowerTwo(inputSize) - inputSize
	println("PADDING SIZE: ", paddingSize)
	var padding strings.Builder
	padding.WriteString(input)
	for i := 0; i < paddingSize; i++ {
		padding.WriteString("X")
	}
	return padding.String(), paddingSize
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
		data[pos+id*size+(size*NumThreads)-1] = y
	}

}

func main() {

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
	var paddingSize int
	input, paddingSize = fixInput(input)
	size := len(input)
	data := make([]TallyType, size*2-1)
	var wg sync.WaitGroup
	for i := 0; i < NumThreads; i++ {
		wg.Add(1)
		go parseInput(i, &wg, input, data, size/NumThreads)
	}
	wg.Wait()
	outputArr := make([]int, size)
	startSum(data, size)
	startSkew(data, outputArr, size)
	outputArr = outputArr[:size-paddingSize]
	size = size - paddingSize
	listIndex := make([]MinSkew, NumThreads)

	for i := 0; i < NumThreads; i++ {
		start := i * size / NumThreads
		end := ((i + 1) * size) / NumThreads
		if end > size {
			end = size
		}
		wg.Add(1)
		go lowestSkewPosition(outputArr, &wg, start, end, &listIndex[i])

	}
	wg.Wait()
	for i := range listIndex {
		println(listIndex[i].index, listIndex[i].value)
	}
	minIndex := MinSkew{-1, math.MaxInt32}
	for i := range listIndex {
		if listIndex[i].value < minIndex.value {
			minIndex.value = listIndex[i].value
			minIndex.index = listIndex[i].index
		}
		println("LISTINDEX: ", listIndex[i].index, listIndex[i].value)
	}
	println(minIndex.index, minIndex.value)
}
