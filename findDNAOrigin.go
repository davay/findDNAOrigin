/*
*@Authors:   Jack Arnold and Devin Lim
*@Algorithm: This program takes in a Archaean genome in a FASTA format and processes it to find likely locations for the dnaA box/origin.
*		     We do this by first cutting down on our search space by calculating the prefix skew (Total Cytosine so far
*            minus Total Guanine so far) . This is done via a recursive, paralellized scan & reduction. There were no
*            performance gains from this process. A parallel search of the prefix skew results was done to find the
*            minimum skew location. It is known that the area around the minimum skew location (window) contains the
*            origin of replication (OriC). We then count the instances of all combinations DNA polymers of K size
*            (k-Mers). The count for a particular k-Mer includes other k-Mers where one character in the string is
*            another nucleotide (hamming distance of 1). To calculate the count for a particular k-Mer we must count for
*            all k-Mers with a hamming distance of 1 (neighbors). This presents an opportunity for parallelization which
*            we took advantage of. A different goroutine is used to count a portion of the neighbors. The most
*            frequently occurring k-Mers are likely candidates for the OriC sequence in the the input genome.
 */

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

const NumThreads = 8
const HalfNumThreads = NumThreads / 2
const ParallelLevels = 3
const WindowSize = 400
const KMerLength = 9

const Letters = "ATGC"

//Return type for the findOriginCandidates
type Candidate struct {
	sequence string
	count    int
}

//Type used to locate the location of least skew
type MinSkew struct {
	index int
	value int
}

//Used to track counts of Cytosine and Guanine in the prefix skew scan/reduction
type TallyType struct {
	c int
	g int
}

//Used to find left child of a node in our logical tree in scan/reduction
func left(i int) int {
	return i*2 + 1
}

//same as left but for right child
func right(i int) int {
	return left(i) + 1
}

//determine if a node is a leaf in our logical tree in scan/reduction
func isLeaf(i int, size int) bool {
	return right(i) >= size*2-1
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
	s.Scan()
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

//Used to get the search region surrounding the index of least skew in the genome
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

//Starts the recursive pair wise summation of Cytosine and Guanine in the genome
func startSum(output []TallyType, size int) {
	var wg sync.WaitGroup
	wg.Add(1)
	calcSum(0, 0, &wg, output, size)
	wg.Wait()
}

//Finds the pair wise summation of Cytosine and Guanine instances in the genome in a recursive parallel fashion
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

//Helper function to calculate pairwise sum when past the ParallelLevel threshold
func seqCalcSum(i int, level int, output []TallyType, size int) {
	if !isLeaf(i, size) {
		seqCalcSum(left(i), level+1, output, size)
		seqCalcSum(right(i), level+1, output, size)
		output[i].c = output[left(i)].c + output[right(i)].c
		output[i].g = output[left(i)].g + output[right(i)].g
	}
}

//Starts the calculation of prefix Skew
func startSkew(input []TallyType, output []int, size int) {
	var wg sync.WaitGroup
	wg.Add(1)
	x := TallyType{0, 0}
	calcSkew(0, x, 0, &wg, input, output, size)
	wg.Wait()
}

//Finds the prefix skew in a recursive parallel fashion
func calcSkew(i int, sumPrior TallyType, level int, wg *sync.WaitGroup, input []TallyType, output []int, size int) {
	defer wg.Done()
	if isLeaf(i, size) {
		output[i-size+1] = (sumPrior.g + input[i].g) - (sumPrior.c + input[i].c)
	} else {
		if level < ParallelLevels-1 {
			var wg2 sync.WaitGroup
			wg2.Add(1)
			go calcSkew(left(i), sumPrior, level+1, &wg2, input, output, size)
			wg2.Wait()
		} else {
			seqCalcSkew(left(i), sumPrior, level+1, input, output, size)
		}
		preSumPrior := TallyType{sumPrior.c + input[left(i)].c, sumPrior.g + input[left(i)].g}
		seqCalcSkew(right(i), preSumPrior, level+1, input, output, size)
	}
}

//Helper function to calculate prefix skew when past the ParallelLevel threshold
func seqCalcSkew(i int, sumPrior TallyType, level int, input []TallyType, output []int, size int) {
	if isLeaf(i, size) {
		output[i-size+1] = (sumPrior.g + input[i].g) - (sumPrior.c + input[i].c)
	} else {
		seqCalcSkew(left(i), sumPrior, level+1, input, output, size)
		preSumPrior := TallyType{sumPrior.c + input[left(i)].c, sumPrior.g + input[left(i)].g}
		seqCalcSkew(right(i), preSumPrior, level+1, input, output, size)
	}
}

//Finds the lowest skew value in a region of the prefix skew map
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

//Finds the index for the lowest prefix skew value in the genome
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

//Takes the reverse compliment of a k-Mer pattern
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

//Counts the instances of a specific k-Mer neighbor in the window
func searchWindowSpecific(window string, patterns []string, count *int, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := range patterns {
		*count = *count + strings.Count(window, patterns[i])
	}
}

//Counts the instances of a k-Mer in the window
func searchWindow(window string, pattern string, revPattern string) int {
	neighbors := make([]string, 0)
	createNeighbors(pattern, &neighbors)
	count := 0
	var wg sync.WaitGroup
	result := make([]int, HalfNumThreads*2)
	revNeighbors := make([]string, 0)
	createNeighbors(revPattern, &revNeighbors)

	for i := 0; i < HalfNumThreads; i++ {
		start := i * len(neighbors) / HalfNumThreads
		end := (i + 1) * len(neighbors) / HalfNumThreads
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

//Gets a "halfway" for the k-Mer search. Because we must search for the reverse k-Mer
func getLastPattern(length int) string {
	var lastPattern strings.Builder
	halfway := int(math.Floor(float64(length) / float64(2)))

	for i := 0; i < length; i++ {
		if halfway > i {
			lastPattern.WriteString("T")
		} else {
			lastPattern.WriteString("C")
		}
	}
	return lastPattern.String()
}

//Finds most frequent K-Mers with reverse compliment and mismatches of 1
func findOriginCandidates(window string, k int) []Candidate {
	lastPattern := getLastPattern(k)
	pattern := getInitialPattern(9)
	countFreq := make(map[string]int, 0)
	for pattern != lastPattern {
		reversePattern := reverse(pattern)
		count := searchWindow(window, pattern, reversePattern)
		countFreq[pattern] = count
		countFreq[reversePattern] = count
		pattern = nextPattern(pattern)
	}
	max := 0
	candidates := make([]Candidate, 0)
	for k, v := range countFreq {
		if v > max {
			candidates = nil
			max = v
			candidates = append(candidates, Candidate{k, v})
		} else if v == max {
			candidates = append(candidates, Candidate{k, v})
		}
	}
	return candidates
}

//Returns a string of A's of length K
func getInitialPattern(length int) string {
	var firstPattern strings.Builder
	for i := 0; i < length; i++ {
		firstPattern.WriteString("A")
	}
	return firstPattern.String()
}

//Returns the next k-Mer to check. AA->TA->GA->CA->AT->AG->AC->TT->TG->TC .... CC
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

//Gets all neighbors of a particular k-Mer
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
	
	if len(os.Args) == 1 {
		println("Wrong input, exitting")
		println("USAGE: ./findDNAOrigin FILENAME")
		os.Exit(1)
	}
	println("INPUT\n-----")
	timeInput := time.Now()

	input, paddingSize := getInput(os.Args[1])
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

	println("FINDORIGIN\n-------")
	timeOriC := time.Now()

	input = input[:len(input)-paddingSize]
	window := getWindow(input, minIndex)
	candidates := findOriginCandidates(window, KMerLength)
	println("TOTAL: ", time.Since(timeOriC).Milliseconds(), "ms\n")

	println("-------------\nTOTAL RUNTIME\n-------------")
	println("TOTAL: ", time.Since(timeInput).Milliseconds(), "ms\n")

	for i := range candidates {
		println("Pattern: ", candidates[i].sequence, " Count: ", candidates[i].count)
	}
}
