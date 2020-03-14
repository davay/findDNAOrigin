package main

import "fmt"

const NumThreads = 16

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
func mapSkew(output []TallyType) []float32 {
	skew := make([]float32, len(output))
	for i := range output {
		cPg := output[i].c + output[i].g
		cMg := output[i].c + output[i].g

		skew[i] = float32(cMg) / float32(cPg)

	}
	return skew

}
func fixInput(input string) string {

	power2 := 1
	for i := 1; i < len(input); i *= 2 {
		power2 = i
	}
	power2 *= 2
	println(power2)
	output := input

	for i := len(input); i < power2; i++ {
		output += "X"
	}
	return output
}

func printData(input []TallyType, size int) {
	for i := 0; i < size; i++ {
		print(i, ": ")
		println("[", input[i].c, ",", input[i].g, "]")

	}
}

func main() {
	input :=
		"aactctatacctcctttttgtcgaatttgtgtgatttatagagaaaatcttattaact" +
			"gaaactaaaatggtaggtttggtggtaggttttgtgtacattttgtagtatctgatttttaattacat" +
			"accgtatattgtattaaattgacgaacaattgcatggaattgaatatatgcaaaacaaacctaccacc" +
			"aaactctgtattgaccattttaggacaacttcagggtggtaggtttctgaagctctcatcaatagact" +
			"attttagtctttacaaacaatattaccgttcagattcaagattctacaacgctgttttaatgggcgtt" +
			"gcagaaaacttaccacctaaaatccagtat"
	input = fixInput(input)

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
	printData(data, size*2-1)
	calcSum(0, 0, data, size)
	println("BEFORE CALCPREFIX")
	calcPrefix(0, x, 0, data, outputArr, size)
	printData(data, size*2-1)
	println("AFTER CALCPREFIX")
	printData(outputArr, size)

	skewMap := mapSkew(outputArr)

	for i := range skewMap {
		fmt.Printf("%f\n", skewMap[i])
	}

}
