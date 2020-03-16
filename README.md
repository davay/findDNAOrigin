# Description
This program takes in a Archaean genome in a FASTA format and processes it to find likely locations for the dnaA box/origin.

We do this by first cutting down on our search space by calculating the prefix skew (Total Cytosine so far minus Total Guanine so far). This is done via a recursive, paralellized scan & reduction.

A parallel search of the prefix skew results was done to find the minimum skew location. It is known that the area around the minimum skew location (window) contains the origin of replication (OriC). We then count the instances of all combinations DNA polymers of K size (k-Mers).

The count for a particular k-Mer includes other k-Mers where one character in the string is another nucleotide (hamming distance of 1, i.e a neighbor). 

This presents an opportunity for parallelization which we took advantage of. A different goroutine is used to count a portion of the neighbors. 

The most frequently occurring k-Mers are likely candidates for the OriC sequence in the the input genome.

Given an input of size 16 million characters, we were able to find a 10% performance improvement going from 1 thread and 1 parallelization level (fork level) to 8 threads and 3 levels.

Proven with E. Coli genome, giving a result of ```TTATCCACA```, as shown [here](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3783049/)
## Howto 

```sh
git clone https://github.com/davay/findDNAOrigin
cd findDNAOrigin
```

### Compile

```go
go build
```


### Run

```GCF_000005845.2_ASM584v2_genomic.fna``` file included is E. Coli's genome
```sh
./findDNAOrigin FILENAME
```
OR
```sh
go run findDNAOrigin.go FILENAME
```

### Getting other genome file (FASTA format only)

[NCBI Genome List](https://www.ncbi.nlm.nih.gov/genome/browse/#!/overview/)
