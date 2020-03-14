package main

import (
	"math"
	"strings"
	"sync"
)

const NumThreads = 1
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
	var wg sync.WaitGroup
	wg.Add(1)
	calcSum(0, 0, &wg, output, size)
	wg.Wait()
}

func calcSum(i int, level int, wg *sync.WaitGroup, output []TallyType, size int) {
	defer wg.Done()
	if !isLeaf(i, size) {
		if level < ParallelLevels-1 {
			var wg2 sync.WaitGroup
			wg2.Add(1)
			go calcSum(left(i), level+1, &wg2, output, size)
			wg2.Wait()
		} else {
			seqCalcSum(left(i), level+1, output, size)
		}
		seqCalcSum(right(i), level+1, output, size)

		output[i].c = output[left(i)].c + output[right(i)].c
		output[i].g = output[left(i)].g + output[right(i)].g

	}
}

func seqCalcSum(i int, level int, output []TallyType, size int) {
	if !isLeaf(i, size) {
		seqCalcSum(left(i), level+1, output, size)
		seqCalcSum(right(i), level+1, output, size)
		output[i].c = output[left(i)].c + output[right(i)].c
		output[i].g = output[left(i)].g + output[right(i)].g
	}
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
		println("C: ", y.c)
		println("G: ", y.g)

		data[pos+id*size+size-1] = y
	}

}

func main() {
	var wg sync.WaitGroup

	// file, err := os.Open(Filename)
	// if err != nil {
	// 	//handle error
	// 	return
	// }
	// defer file.Close()
	// var input string
	// s := bufio.NewScanner(file)
	// for s.Scan() {
	// 	input += s.Text()
	// }
	input := "CCAAATTTGCGGGGG"
	input = fixInput(input)
	println("INPUT: ", input)
	size := len(input)
	data := make([]TallyType, size*2-1)
	for i := 0; i < NumThreads; i++ {
		wg.Add(1)
		go parseInput(i, &wg, input, data, size/NumThreads)
	}
	wg.Wait()

	println("ORIGINAL")
	printData(data, size*2-1)

	// outputArr := make([]int, size)

	startSum(data, size)

	println("AFTER SUM")
	printData(data, size*2-1)

	// startSkew(data, outputArr, size)

}
