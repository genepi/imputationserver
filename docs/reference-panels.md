# Reference Panels

Our server offers imputation from the following reference panels:


## HRC (Version r1.1 2016)

The HRC panel consists of 64,940 haplotypes of predominantly European ancestry.

| ||
| | |
| Number of Samples | 32,470 |
| Sites (chr1-22) | 39,635,008 |
| Chromosomes | 1-22, X|
| Website | [http://www.haplotype-reference-consortium.org](http://www.haplotype-reference-consortium.org); [HRC r1.1 Release Note](https://imputationserver.sph.umich.edu/start.html#!pages/hrc-r1.1) |

## 1000 Genomes Phase 3 (Version 5)

Phase 3 of the 1000 Genomes Project consists of 5,008 haplotypes from 26 populations across the world.

| ||
| | |
| Number of Samples | 2,504 |
| Sites (chr1-22) | 49,143,605 |
| Chromosomes | 1-22, X|
| Website | [http://www.internationalgenome.org](http://www.internationalgenome.org) |



## Genome Asia Pilot - GAsP

| ||
| | |
| Number of Samples | 1,654 |
| Sites (chr1-22) | 21,494,814 |
| Chromosomes | 1-22 |
| Publication | [https://www.nature.com/articles/s41586-019-1793-z](https://www.nature.com/articles/s41586-019-1793-z) |

## Genome Asia v2 - GAsP

| ||
| | |
| Number of Samples | 6,461 |
| Sites (chr1-22) | - |
| Chromosomes | 1-22 |
| Publication | [https://www.nature.com/articles/s41586-019-1793-z](https://www.nature.com/articles/s41586-019-1793-z) |


## Four-digit Multi-ethnic HLA v1 (2021)

The multi-ethnic HLA panel consists of 36,586 haplotypes of five global populations. We have released the panel at both two-field (four-digit) and G-group resolution.

| ||
| | |
| Number of Samples | 18,293 |
| Sites (chr6) | 56,310 (1,781 HLA alleles; 4,513 HLA amino acids; 10,924 SNPs within HLA; 39,092 scaffold SNPs) |
| Chromosomes | 6:28,000,000-34,000,000 |
| Website | [https://github.com/immunogenomics/HLA-TAPAS/](https://github.com/immunogenomics/HLA-TAPAS/) |

Please cite this manuscript if you would like to include imputed results from the panel in  your work:

Luo, Y., Kanai, M., Choi, W., Li, X., Yamamoto, K., Ogawa, K., Gutierrez-Arcelus, M., Gregersen, P. K., Stuart, P. E., Elder, J. T., Fellay, J., Carrington, M., Haas, D. W., Guo, X., Palmer, N. D., Chen, Y.-D. I., Rotter, J. I., Taylor, K. D., Rich, S., … Raychaudhuri, S. (2020). A high-resolution HLA reference panel capturing global population diversity enables multi-ethnic fine-mapping in HIV host response. https://doi.org/10.1101/2020.07.16.20155606

## Four-digit Multi-ethnic HLA v2 (2022)

| ||
| | |
| Number of Samples | 20,349 |
| Sites (chr6) | 22,733 (570 HLA alleles; 3,449 HLA amino acids; 4,023 SNPs within HLA; 14,691 scaffold SNPs) |
| Chromosomes | 6:27,970,031-33,965,553 |
| Website | [https://github.com/immunogenomics/HLA-TAPAS/](https://github.com/immunogenomics/HLA-TAPAS/) |

### Multi-ethnic HLA v2 Details

**Samples:**

  - We added Japanese samples with HLA alleles called with deep target sequencing (n = 723; Hirata et al. Nat Genet. 2019).
  - Add Estonian biobank samples with IRB approval (n = 2,233; Mitt et al. Eur J Hum Genet. 2017).

**HLA alleles:**

  - We used gold-standard HLA allele calls (instead of inferred calls from whole genome sequencing) when possible.
  - We updated and recalled HLA alleles using HLA-LA (Dilthey et al. Bioinformatics. 2019) in Jackson Heart Study (n = 3,026).
  - We updated and recalled HLA alleles using HLA-LA in Estonian Biobank (n = 2,233).

**Scaffold variants:**

  - We added population-specific SNP variants which are common in one population but rare in the other populations when they are in 1KG and pass QC, by cross-cohort SNP imputation within the reference panel.
  - We removed all SNP variants that are not included in the commonly-used genotyping arrays.

**Minimac4 options:**

  - We optimized `--probThreshold`, `--diffThreshold`, `--topThreshold` parameters in minimac4. Optimized parameters are now used for all HLA panels.

**Imputation results:**

  - We output 2-digit and 4-digit-resolution HLA alleles. We do not output higher resolution HLA alleles in this version.

## CAAPA - African American Panel

  Whole genome sequences were available on 883 individuals from 19 case-control studies of asthma included in the Consortium on Asthma among African-ancestry Populations in the Americas (CAAPA) and were used to summarize the genomic contributions to individuals of African ancestry.

  | ||
  | | |
  | Number of Samples | 883 |
  | Sites (chr1-22) | 31,163,897 |
  | Chromosomes | 1-22 |
  | Website | [http://www.caapa-project.org/](http://www.caapa-project.org/). Further details can also be found [here](https://imputationserver.sph.umich.edu/start.html#!pages/caapa) |


## HRC (Version r1 2015)

This HRC panel consists of 64,976 haplotypes of predominantly European ancestry.

| ||
| | |
| Number of Samples | 32,488 |
| Sites (chr1-22) | 39,741,659 |
| Chromosomes | 1-22, X|
| Website | [http://www.haplotype-reference-consortium.org](http://www.haplotype-reference-consortium.org); [HRC r1 Release Note](https://imputationserver.sph.umich.edu/start.html#!pages/hrc) |

## 1000 Genomes Phase 1 (Version 3)

| ||
| | |
| Number of Samples | 1,092 |
| Sites (chr1-22) | 28,975,367 |
| Chromosomes | 1-22, X|
| Website | [http://www.internationalgenome.org](http://www.internationalgenome.org) |

## HapMap 2

| ||
| | |
| Number of Samples | 60 |
| Sites (chr1-22) | 2,542,916 |
| Chromosomes | 1-22 |
| Website: | [http://www.hapmap.org](http://www.hapmap.org) |
