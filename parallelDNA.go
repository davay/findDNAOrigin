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
const WindowSize = 250
const Letters = "ATGC"

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
	file, err := os.Open(filename)
	if err != nil {
		println("Can't find ", filename)
		os.Exit(1)
	}
	defer file.Close()
	s := bufio.NewScanner(file)
	var input string
	for s.Scan() {
		input += s.Text()
	}
	return fixInput(input)
}

func processInput(input string) []TallyType {
	size := len(input)
	data := make([]TallyType, size*2-1)
	var wg sync.WaitGroup
	for i := 0; i < NumThreads; i++ {
		wg.Add(1)
		go parseInput(i, &wg, input, data, size/NumThreads)
	}
	wg.Wait()
	return data
}

func fixInput(input string) (string, int) {
	inputSize := len(input)
	paddingSize := findNextPowerTwo(inputSize) - inputSize
	var padding strings.Builder
	padding.WriteString(input)
	for i := 0; i < paddingSize; i++ {
		padding.WriteString("X")
	}
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

//~~~~~~~~~~~~~// Next Algorithm

func fetchMostFrequentPattern(genome string, k int, mismatches *int) []string {
	patterns := make([]string, 0)
	pattern := getInitialPattern(k)
	lastPattern := getLastPattern(k)
	reversePattern := reverse(pattern)
	patterns = append(patterns, pattern)
	max := fetchApproximateMatchingCount(pattern, genome, mismatches)
	max += fetchApproximateMatchingCount(reversePattern, genome, mismatches)

	for pattern != lastPattern {
		pattern = nextPattern(pattern)
		reversePattern = reverse(pattern)
		occurredCount := fetchApproximateMatchingCount(pattern, genome, mismatches)
		occurredCount += fetchApproximateMatchingCount(reversePattern, genome, mismatches)

		if occurredCount > max {
			patterns = nil
			patterns = append(patterns, pattern)
			max = occurredCount
		} else if occurredCount == max {
			patterns = append(patterns, pattern)
		}
	}
	return patterns
}

func nextPattern(pattern string) string {
	var nextP strings.Builder

	for i := len(pattern) - 1; i >= 0; i-- {
		if pattern[i] == Letters[len(Letters)-1] {
			continue
		}
		index := strings.Index(Letters, string(pattern[i])) + 1
		nextP.WriteString(pattern[0:i])
		nextP.WriteString(string(Letters[index]))

		for j := i + 1; j < len(pattern); j++ {
			nextP.WriteString(string(Letters[0]))
		}
		break
	}
	return nextP.String()
}

func getInitialPattern(length int) string {
	var firstPattern strings.Builder
	for i := 0; i < length; i++ {
		firstPattern.WriteString("A")
	}
	return firstPattern.String()
}

func getLastPattern(length int) string {
	var lastPattern strings.Builder
	for i := 0; i < length; i++ {
		lastPattern.WriteString("C")
	}
	return lastPattern.String()
}

//NOTE THIS CAN BE PARALLELIZED!!!
func fetchApproximateMatchingCount(pattern string, genome string, mismatches *int) int {
	matchingCount := 0
	for i := 0; i < len(genome)-len(pattern); i++ {
		substring := genome[i : i+len(pattern)]
		if isApproximateMatching(pattern, substring, mismatches) {
			matchingCount++
		}
	}
	return matchingCount
}

//NOTE MISMATCHES ---> In Java does this get changed when we are r
func isApproximateMatching(pattern string, substring string, mismatches *int) bool {
	for i := 0; i < len(pattern); i++ {
		if string(pattern[i]) != string(substring[i]) {
			currMismatch := *mismatches
			currMismatch--
			mismatches = &currMismatch
		}
		if *mismatches < 0 {
			return false
		}
	}
	return true
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

func main() {
	input, paddingSize := getInput(Filename)
	data := processInput(input)
	size := len(input)
	outputArr := make([]int, size)
	startSum(data, size)
	startSkew(data, outputArr, size)
	minIndex := findMin(outputArr, size, paddingSize)
	println(minIndex)
	input = input[:len(input)-paddingSize]
	window := getWindow(input, minIndex)
	mismatches := 1
	patterns := fetchMostFrequentPattern(window, 9, &mismatches)
	for i := range patterns {
		println(patterns[i])
	}
}
