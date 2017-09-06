# Data preparation

Imputationserve accepts VCF files compressed by [bgzip](http://samtools.sourceforge.net/tabix.shtml). Please make sure the following requirements have been met:

- Create a seperate vcf.gz file for each chromosome.
- Variations must be sorted by genomic position.
- GRCh37 coordinates are required.

!!! note
    Several .vcf.gz files can be uploaded at once.

## Convert ped/map files to VCF files

To convert your ped/map file into a VCF file, please use either [plink2](https://www.cog-genomics.org/plink2/), [VCFtools](http://vcftools.sourceforge.net/man_latest.html) or [VcfCooker](http://genome.sph.umich.edu/wiki/VcfCooker).  

````sh
plink --ped study_chr1.ped --map study_chr1.map --recode vcf --out study_chr1
````

Create a sorted vcf.gz file using [VCFtools](http://vcftools.sourceforge.net) and [](http://sourceforge.net/projects/samtools/files/tabix/)[tabix (including bgzip)](http://sourceforge.net/projects/samtools/files/tabix/):

````sh
vcf-sort mystudy_chr1.vcf | bgzip -c > mystudy_chr1.vcf.gz
````

## CheckVCF

Use [checkVCF](https://github.com/zhanxw/checkVCF) to ensure that the VCF files are valid. checkVCF proposes "Action Items" (e.g. upload to sftp server), which can be ignored. Only the validity should be checked with this command.

````sh
checkVCF.py -r human_g1k_v37.fasta -o out mystudy_chr1.vcf.gz
````

## QC for HRC imputation

Will Rayner provides a great toolbox to prepare data for HRC imputation: [HRC preparation checking Tool](http://www.well.ox.ac.uk/~wrayner/tools/).
