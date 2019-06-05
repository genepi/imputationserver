# Create a Reference Panel

This tutorial will help you to create your own reference panel and integrate it into Michigan Imputation Server. 

## Required Software
- To create m3vcf files for imputation, please use [Minimac3](https://github.com/Santy-8128/Minimac3).
- To create bcf files for phasing, please use [bcftools](https://samtools.github.io/bcftools/bcftools.html) and tabix.
- To create legend files for QC, please use vcftools.

## Folder Structure

We recommend the following folder structure:

```ansi
my-ref-panel
├── cloudgene.yaml
├── bcfs
|   ├── chr1.bcf
    ├── chr1.bcf.csi
|   ├── ...
    ├── chr22.bcf
|   └── chr22.bcf.csi
├── legends
|   ├── chr1.legend.gz
|   ├── ...
|   └── chr22.legend.gz
├── m3vcfs
|   ├── chr1.m3vcf.gz
|   ├── ...
|   └── chr22.m3vcf.gz
├── map
|   └── genetic_map_hg19_withX.txt.gz
└── README.md
```


## Init

Create a new folder and create a `cloudgene.yaml` file:

```
name:  My Reference Panel name
description: a short description
version: 1.0.0
website: http://my-reference-panel.com

properties:
  id: hapmap
  hdfs: ${hdfs_app_folder}/m3vcfs/chr$chr.m3vcf.gz
  legend: ${local_app_folder}/legends/chr$chr.legend.gz
  mapEagle: ${hdfs_app_folder}/map/genetic_map_hg19_withX.txt.gz
  refEagle: ${hdfs_app_folder}/bcfs/chr$chr.bcf
  build: hg19

installation:

  - import:
      source: ${local_app_folder}/bcfs
      target: ${hdfs_app_folder}/bcfs

  - import:
      source: ${local_app_folder}/m3vcfs
      target: ${hdfs_app_folder}/m3vcfs

  - import:
      source: ${local_app_folder}/map
      target: ${hdfs_app_folder}/map
```


## Create bcf files
BCF files are required for phasing with [eagle](https://data.broadinstitute.org/alkesgroup/Eagle/).
```sh
for CHR in `seq 1 22`
do
    bcftools view chr${CHR}.vcf.gz -O b -o chr${CHR}.bcf
    bcftools index chr${CHR}.bcf
done
```
## Create m3vcf files

m3vcf files are used to store large reference panels in a compact way. Learn more about the file format [here](https://genome.sph.umich.edu/wiki/M3VCF_Files).

```sh
for CHR in `seq 1 22`
do
    Minimac3 --refHaps chr${CHR}.vcf.gz --processReference --prefix m3vcfs/chr${CHR}
done
```


## Create legend files

A legend file is a tab-delimited file consisting of 5 columns (`id`, `position`, `a0`, `a1`, `population.aaf`). It is used by Michigan Imputation Server to create QC Statistics. 
`a0` and `a1` including the ref/alt alleles, `population.aaf` the alternate allele frequency. 
Please note that `population` must be substituted by the actual population (e.g. eur). This population is then specified in the [minimac4.yaml file](https://github.com/genepi/imputationserver/blob/master/files/minimac4.yaml). 
A legend file for chr20 can be found [here](https://github.com/genepi/imputationserver/blob/master/test-data/configs/hapmap-chr20/ref-panels/hapmap_r22.chr20.CEU.hg19_impute.legend.gz?raw=true).

Please execute the following commands (starting from vcf files) to create the legend files:

```sh
for CHR in `seq 1 22`
do
    vcftools --gzvcf chr${CHR}.vcf.gz --freq --out chr$i
    sed 's/:/\t/g' chr$i.frq | sed 1d | awk '{print $1":"$2" "$2" "$5" "$7" "$8}' > chr$i.legend
    cat <header.txt> chr$i.legend | bgzip > chr$i.legend.gz
done
```


## Reference genetic maps

The genetic maps for eagle (in our case hg19) can be found [here](https://data.broadinstitute.org/alkesgroup/Eagle/downloads/tables/genetic_map_hg19_withX.txt.gz).

## Integrate your new reference panel
The created folder structure must be compressed to a zip archive and can now be integrated into Michigan Imputation Server. Please go to [this page](http://imputationserver.readthedocs.io/en/latest/docker/#install-1000g-phase-3-reference-panel) for further instructions. A full working zip archive for Hapmap can be found [here](https://imputationserver.sph.umich.edu/static/downloads/releases/hapmap2-1.0.0.zip).


