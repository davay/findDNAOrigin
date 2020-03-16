package main

import (
	"bufio"
	"math"
	"os"
	_ "regexp"
	"strings"
	"sync"
	"time"
)

const NumThreads = 1
const ParallelLevels = 1
const Filename = "genome"
const WindowSize = 500
const Letters = "ATGC"

type CandidateString struct {
	sequence string
	count    int
}
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

func printData(input []TallyType, size int) {
	for i := 0; i < size; i++ {
		print(i, ": ")
		println("[", input[i].c, ",", input[i].g, "]")
	}
}

func getInput(filename string) (string, int) {
	start := time.Now()
	file, err := os.Open(filename)
	if err != nil {
		println("Can't find ", filename)
		os.Exit(1)
	}
	defer file.Close()
	s := bufio.NewScanner(file)
	var inputBuilder strings.Builder
	for s.Scan() {
		inputBuilder.WriteString(s.Text())
	}
	elapsed := time.Since(start)
	println("getInput took: ", elapsed.Milliseconds(), "ms")
	return fixInput(inputBuilder.String())
}

func processInput(input string) []TallyType {
	start := time.Now()
	size := len(input)
	data := make([]TallyType, size*2-1)
	var wg sync.WaitGroup
	for i := 0; i < NumThreads; i++ {
		wg.Add(1)
		go parseInput(i, &wg, input, data, size/NumThreads)
	}
	wg.Wait()
	elapsed := time.Since(start)
	println("processInput took: ", elapsed.Milliseconds(), "ms")
	return data
}

func fixInput(input string) (string, int) {
	start := time.Now()
	inputSize := len(input)
	paddingSize := findNextPowerTwo(inputSize) - inputSize
	var padding strings.Builder
	padding.WriteString(input)
	for i := 0; i < paddingSize; i++ {
		padding.WriteString("X")
	}
	elapsed := time.Since(start)
	println("fixInput took: ", elapsed.Milliseconds(), "ms")
	return padding.String(), paddingSize
}

func parseInput(id int, wg *sync.WaitGroup, input string, data []TallyType, size int) {
	defer wg.Done()
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

func getWindow(input string, index int) string {
	start := 0
	end := len(input)
	if index-WindowSize > -1 {
		start = index - WindowSize
	}
	if index+WindowSize < len(input) {
		end = index + WindowSize
	}
	return input[start:end]
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

func findMin(outputArr []int, size int, paddingSize int) int {
	var wg sync.WaitGroup
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
	minIndex := MinSkew{-1, math.MaxInt32}
	for i := range listIndex {
		if listIndex[i].value < minIndex.value {
			minIndex.value = listIndex[i].value
			minIndex.index = listIndex[i].index
		}
	}
	return minIndex.index
}

func reverse(pattern string) string {
	var reversed strings.Builder
	length := len(pattern) - 1
	for i := length; i >= 0; i-- {
		switch string(pattern[i]) {
		case "A":
			reversed.WriteString("T")
		case "T":
			reversed.WriteString("A")
		case "G":
			reversed.WriteString("C")
		case "C":
			reversed.WriteString("G")
		}
	}
	return reversed.String()
}

func getAllKLength(set string, prefix string, k int, combos *[]string) {
	if k == 0 {
		*combos = append(*combos, prefix)
		return
	}
	for i := range set {
		var combo strings.Builder
		combo.WriteString(prefix)
		combo.WriteByte(set[i])
		getAllKLength(set, combo.String(), k-1, combos)
	}
}

func searchWindowSpecific(window string, patterns []string, count *int, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := range patterns {
		*count = *count + strings.Count(window, patterns[i])
	}
}

func searchWindow(window string, pattern string, revPattern string) int {
	neighbors := make([]string, 0)
	createNeighbors(pattern, &neighbors)
	count := 0
	var wg sync.WaitGroup
	result := make([]int, NumThreads*2)
	revNeighbors := make([]string, 0)
	createNeighbors(revPattern, &revNeighbors)

	for i := 0; i < NumThreads; i++ {
		start := i * len(neighbors) / NumThreads
		end := (i + 1) * len(neighbors) / NumThreads
		if end > len(neighbors) {
			end = len(neighbors)
		}
		wg.Add(2)
		go searchWindowSpecific(window, neighbors[start:end], &result[i], &wg)
		go searchWindowSpecific(window, revNeighbors[start:end], &result[i+1], &wg)
	}
	wg.Wait()

	for i := range result {
		count += result[i]
	}

	return count
}

func findFreqKLengthPatterns(window string, k int) {
	combos := make([]string, 0)
	getAllKLength("ATGC", "", k, &combos)
	countFreq := make(map[string]int, len(combos))

	//initialize k-v pairs with 0 values
	for i := range combos {
		countFreq[combos[i]] = 0
	}

	for i := range countFreq {
		if countFreq[i] == 0 {
			//DON"T SEARCH FOR REVERSE LATER
			reversePattern := reverse(i)
			count := searchWindow(window, i, reversePattern)
			countFreq[i] = count
			countFreq[reversePattern] = count
		}
	}

	max := 0
	candidates := make([]CandidateString, 0)

	for k, v := range countFreq {
		if v > max {
			candidates = nil
			max = v
			candidates = append(candidates, CandidateString{k, v})
		} else if v == max {
			candidates = append(candidates, CandidateString{k, v})
		}
	}

	//FIXME move print statement
	for i := range candidates {
		println("Pattern: ", candidates[i].sequence, " Count: ", candidates[i].count)
	}
}

func createNeighbors(pattern string, neighbors *[]string) {
	for i := range pattern {
		for j := range Letters {
			if i == 0 {
				*neighbors = append(*neighbors, string(Letters[j])+pattern[1:])
			}
			if i == len(pattern)-1 {
				*neighbors = append(*neighbors, pattern[0:i]+string(Letters[j]))
			}
			if i != 0 && i != len(pattern)-1 {
				*neighbors = append(*neighbors, pattern[0:i]+string(Letters[j])+pattern[i+1:])
			}
		}
	}
}

func main() {
	println("INPUT\n-----")
	timeInput := time.Now()

	input, paddingSize := getInput(Filename)
	data := processInput(input)

	println("TOTAL: ", time.Since(timeInput).Milliseconds(), "ms\n")

	println("SUM\n---")
	timeSum := time.Now()

	size := len(input)
	startSum(data, size)

	println("TOTAL: ", time.Since(timeSum).Milliseconds(), "ms\n")

	println("SKEW\n----")
	timeSkew := time.Now()

	outputArr := make([]int, size)
	startSkew(data, outputArr, size)

	println("TOTAL: ", time.Since(timeSkew).Milliseconds(), "ms\n")

	println("FINDMIN\n-------")
	timeMin := time.Now()

	minIndex := findMin(outputArr, size, paddingSize)

	println("TOTAL: ", time.Since(timeMin).Milliseconds(), "ms\n")

	println("FINDORIC\n-------")
	timeOriC := time.Now()

	input = input[:len(input)-paddingSize]
	window := getWindow(input, minIndex)
	findFreqKLengthPatterns(window, 9)

	println("TOTAL: ", time.Since(timeOriC).Milliseconds(), "ms\n")
}
