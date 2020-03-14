package main

import (
	"fmt"
	"io/ioutil"
	"math"
)

const NumThreads = 16
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

func calcSum(i int, level int, output []TallyType, size int) {

	if !isLeaf(i, size) {
		calcSum(left(i), level+1, output, size)
		calcSum(right(i), level+1, output, size)
		output[i].c = output[left(i)].c + output[right(i)].c
		output[i].g = output[left(i)].g + output[right(i)].g
	}
}

func calcPrefix(i int, sumPrior TallyType, level int, input []TallyType, output []TallyType, size int) {
	if isLeaf(i, size) {
		output[i-size+1].c = sumPrior.c + input[i].c
		output[i-size+1].g = sumPrior.g + input[i].g
	} else {
		calcPrefix(left(i), sumPrior, level+1, input, output, size)
		preSumPrior := TallyType{sumPrior.c + input[left(i)].c, sumPrior.g + input[left(i)].g}
		calcPrefix(right(i), preSumPrior, level+1, input, output, size)
	}
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

func fixInput(input string) (string, int) {

	power2 := 1
	for i := 1; i < len(input); i *= 2 {
		power2 = i
	}
	power2 *= 2
	println(power2)
	output := input
	xCount := 0
	for i := len(input); i < power2; i++ {
		output += "X"
		xCount++
	}
	return output, xCount
}

func printData(input []TallyType, size int, xCount int) {
	for i := 0; i < size-xCount; i++ {
		print(i, ": ")
		println("[", input[i].c, ",", input[i].g, "]")

	}
}

func main() {
	content, err := ioutil.ReadFile(Filename)
	if err != nil {
		fmt.Println("File reading error", err)
		return
	}
	input, xCount := fixInput(string(content))

	size := len(input)
	data := make([]TallyType, size*2-1)
	for pos, char := range input {
		y := TallyType{0, 0}
		if char == 'c' {
			y.c = 1
		}
		if char == 'g' {
			y.g = 1
		}
		data[pos+size-1] = y
	}

	x := TallyType{0, 0}
	outputArr := make([]TallyType, size)
	println("BEFORE CALCSUM")
	printData(data, size*2-1, xCount)
	calcSum(0, 0, data, size)
	println("BEFORE CALCPREFIX")
	calcPrefix(0, x, 0, data, outputArr, size)
	printData(data, size*2-1, xCount)
	println("AFTER CALCPREFIX")
	printData(outputArr, size, xCount)

	skewMap := mapSkew(outputArr, xCount)

	for i := range skewMap {
		fmt.Printf("%f\n", skewMap[i])
	}

}
