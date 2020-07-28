# Pipeline Overview

Our pipeline performs the following steps:

## Quality Control

*  Create chunks with a size of 20 Mb
*  For each 20Mb chunk we perform the following checks:

    **On Chunk level:**

    *   Determine amount of valid variants: A variant is valid iff it is included in the reference panel. At least 3 variants must be included.
    *   Determine amount of variants found in the reference panel: At least 50 % of the variants must be be included in the reference panel.
    *   Determine sample call rate: At least 50 % of the variants must be called for each sample.  

    Chunk exclusion: if (#variants < 3 || overlap < 50% || sampleCallRate < 50%)

    **On Variant level:**

    *   Check alleles: Only A,C,G,T are allowed
    *   Calculate alternative allele frequency (AF): Mark all with a AF > 0.5.
    *   Calculate SNP call rate
    *   Calculate chi square for each variant (reference panel vs. study data)
    *   Determine allele switches: Compare ref and alt of reference panel with study data (A/T and C/G variants are ignored).
    *   Determine strand flips: After eliminating possible allele switches, flip and compare ref/alt from reference panel with study data.
    *   Determine allele switches in combination with strand flips: Combine the two rules from above.  

    Variant exclusion: Variants are excluded in case of: [a] invalid alleles occur (!(A,C,G,T)), [b] duplicates (DUP filter or (pos - 1 == pos)), [c] indels, [d] monomorphic sites, [e] allele mismatch between reference panel and study, [f] SNP call rate < 90%.

    **On Sample level:**

    *   For chr1-22, a chunk is excluded if one sample has a call rate < 50 %. Only complete chunks are excluded, not samples (see "On Chunk level" above)


* Perform a liftOver step, if build of input data and reference panel does not match (b37 vs b38).

## Phasing

* Execute for each chunk one of the following phasing algorithms (we use an overlap of 5 Mb). For example, chr20:1-20000000 and reference population EUR:

**Eagle2**
````sh
./eagle --vcfRef HRC.r1-1.GRCh37.chr20.shapeit3.mac5.aa.genotypes.bcf
--vcfTarget chunk_20_0000000001_0020000000.vcf.gz  --geneticMapFile genetic_map_chr20_combined_b37.txt
--outPrefix chunk_20_0000000001_0020000000.phased --bpStart 1 --bpEnd 25000000 -allowRefAltSwap
--vcfOutFormat z
````
    
**Please note:** Target-only sites for unphased data are not included in the final output.
    
## Imputation

###
* Execute for each chunk minimac in order to impute the phased data (we use a window of 500 kb):

````sh
./Minimac4 --refHaps HRC.r1-1.GRCh37.chr1.shapeit3.mac5.aa.genotypes.m3vcf.gz
--haps chunk_1_0000000001_0020000000.phased.vcf --start 1 --end 20000000
--window 500000 --prefix chunk_1_0000000001_0020000000 --cpus 1 --chr 20 --noPhoneHome
--format GT,DS,GP --allTypedSites --meta --minRatio 0.00001
````
If a map file is available (currently TOPMed only), the following cmd is executed:

````sh
./Minimac4 --refHaps HRC.r1-1.GRCh38.chr1.shapeit3.mac5.aa.genotypes.m3vcf.gz
--haps chunk_1_0000000001_0020000000.phased.vcf --start 1 --end 20000000
--window 500000 --prefix chunk_1_0000000001_0020000000 --cpus 1 --chr 20 --noPhoneHome
--format GT,DS,GP --allTypedSites --meta --minRatio 0.00001 --referenceEstimates --map B38_MAP_FILE.map
````
## Compression and Encryption

* Merge all chunks of one chromosome into one single vcf.gz
* Encrypt data with one-time password

## Chromosome X Pipeline

Additionally to the standard QC, the following per-sample checks are executed for chrX:

* Ploidy Check: Verifies if all variants in the nonPAR region are either haploid or diploid.
* Mixed Genotypes Check: Verifies if the amount of mixed genotypes (e.g. 1/.) is < 10 %.

For phasing and imputation, chrX is split into three independent chunks (PAR1, nonPAR, PAR2). These splits are then automatically merged by Michigan Imputation Server and are returned as one complete chromosome X file. Only Eagle is supported.
