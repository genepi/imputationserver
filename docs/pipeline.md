# Pipeline Details

Our pipeline performs the following steps:

1.  Create chunks with a size of 20 Mb
2.  For each 20Mb chunk we perform the following checks:

    **On Chunk level:**

    *   Determine amount of valid variants: A variant is valid iff it is included in the reference panel. At least 3 variants must be included.
    *   Determine amount of variants found in the reference panel: At least 50 % of the variants must be be included in the reference panel.
    *   Determine sample call rate: At least 50 % of the variants must be called for each sample.  

    Chunk exclusion: if (#variants < 3 || #foundVariants < 50% || sampleCallRate < 50%)

    **On Variant level:**

    *   Check alleles: Only A,C,G,T are allowed
    *   Calculate alternative allele frequency (AF): Mark all with a AF > 0.5.
    *   Calculate SNP call rate
    *   Calculate chi square for each variant (reference panel vs. study data)
    *   Determine allele switches: Compare ref and alt of reference panel with study data (A/T and C/G variants are ignored).
    *   Determine strand flips: After eliminating possible allele switches, flip and compare ref/alt from reference panel with study data.
    *   Determine allele switches in combination with strand flips: Combine the two rules from above.  

    Variant exclusion: Variants are excluded in case of: [a] invalid alleles occur (!(A,C,G,T)), [b] duplicates (DUP filter or (pos - 1 == pos)), [c] indels, [d] monomorphic sites, [e] not included in reference panel, [f] allele mismatch between reference panel and study, [g] SNP call rate < 90%.

    **On Sample level:**

    *   For chrX, check if sex can be determined. Exclude sample otherwise.
    *   For chr1-22, a chunk is excluded if one sample has a call rate < 50 %. Only complete chunks are excluded, not samples (see "On Chunk level" above)

3. Perform a liftOver step, if input is b37 (HG19) and the Topmed panel (b38) is selected.

4.  Execute for each chunk one of the following phasing algorithms (we use an overlap of 5 Mb). For example, chr20:1-20000000 and reference population EUR:

    **Eagle2**
      <pre>./eagle --vcfRef HRC.r1-1.GRCh37.chr20.shapeit3.mac5.aa.genotypes.bcf --vcfTarget chunk_20_0000000001_0020000000.vcf.gz  --./geneticMapFile genetic_map_chr20_combined_b37.txt --outPrefix chunk_20_0000000001_0020000000.phased --bpStart 1 --bpEnd 25000000 -allowRefAltSwap --vcfOutFormat z --outputUnphased
      </pre>

    **ShapeIt**
    <pre>./vcfCooker --in-vcf chunk_20_0000000001_0020000000.vcf --write-bed --out chunk_20_0000000001_0020000000
    ./shapeit --input-bed chunk_20_0000000001_0020000000.bed chunk_20_0000000001_0020000000.bim chunk_20_0000000001_0020000000.fam --input-map genetic_map_b37.tar.gz/genetic_map_chr20_combined_b37.txt --output-max chunk_20_0000000001_0020000000.phased --input-from 1 --input-to 25000000 --effective-size 11418
    </pre>

    **HapiUR**
    <pre>./vcfCooker --in-vcf chunk_20_0000000001_0020000000.vcf --write-bed --out chunk_20_0000000001_0020000000
    ./insert-map.pl chunk_20_0000000001_0020000000.bim genetic_map_chr20_combined_hapiur_b37.txt
    ./hapi-ur -g chunk_20_0000000001_0020000000.bed -s chunk_20_0000000001_0020000000.map.bim -i chunk_20_0000000001_0020000000.fam -w 73 -o chunk_20_0000000001_0020000000 -c 20 --start 1 --end 25000000 --impute2
    </pre>


5.  Execute for each chunk minimac in order to impute the phased data (we use a window of 500 kb)

    <pre>./Minimac4 --refHaps HRC.r1-1.GRCh37.chr1.shapeit3.mac5.aa.genotypes.m3vcf.gz --haps chunk_1_0000000001_0020000000.phased.vcf --start 1 --end 20000000 --window 500000 --prefix chunk_1_0000000001_0020000000 --chr 20 --noPhoneHome --format GT,DS,GP --allTypedSites --constantPara 1.9e-05 --minRatio 0.00001 --map B37_MAP_FILE.map
    </pre>

6.  Merge all chunks of one chromosome into one single vcf
7.  Encrypt data with one-time password

## Chromosome X

For phasing and imputation, chrX is split into three independent chunks (PAR1, nonPAR, PAR2). These splits are then automatically merged by Michigan Imputation Server and are returned as one complete chromosome X file. Only Eagle is supported.

### b37 coordinates
| | |
| ChrX PAR1 Region | chr X1 (< 2699520) |
| ChrX nonPAR Region | chr X2 (2699520 - 154931044) |
| ChrX PAR2 Region | chr X3 (> 154931044) |

### b38 coordinates
| | |
| ChrX PAR1 Region | chr X1 (< 2781479) |
| ChrX nonPAR Region | chr X2 (2781479 - 155701383) |
| ChrX PAR2 Region | chr X3  (> 155701383)|

Additionally to the standard QC, the following per-sample checks are executed for chrX:

1. Ploidy Check: Verifies if all variants in the nonPAR region are either haploid or diploid.
2. Mixed Genotypes Check: Verifies if the amount of mixed genotypes (e.g. 1/.) are < 10 %.
