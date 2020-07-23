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

Create a new folder and create a `cloudgene.yaml` file. 

```
name:  My Reference Panel name
id: unique-id
description: a short description
category: RefPanel
version: 1.0.0
website: http://my-reference-panel.com

properties:
  hdfs: ${hdfs_app_folder}/m3vcfs/chr$chr.m3vcf.gz
  legend: ${local_app_folder}/legends/chr$chr.legend.gz
  mapEagle: ${hdfs_app_folder}/map/genetic_map_hg19_withX.txt.gz
  refEagle: ${hdfs_app_folder}/bcfs/chr$chr.bcf
  build: hg19
  samples:
    all: 2504
    mixed: -1
  populations:
    all: ALL
    mixed: Other/Mixed
    
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
For hg38 also set ``mapMinimac: ${app_hdfs_folder}/map/geneticMapFile.b38.map.txt`` and  ``mapEagle: ${app_hdfs_folder}/map/genetic_map_hg38_withX.txt.gz``.

## Create bcf files
BCF files are required for phasing with [eagle](https://data.broadinstitute.org/alkesgroup/Eagle/).
```sh
for chr in `seq 1 22`
do
    bcftools view chr${chr}.vcf.gz -O b -o chr${chr}.bcf
    bcftools index chr${chr}.bcf
done
```
## Create m3vcf files

m3vcf files are used to store large reference panels in a compact way. Learn more about the file format [here](https://genome.sph.umich.edu/wiki/M3VCF_Files).

```sh
for chr in `seq 1 22`
do
    Minimac3 --refHaps chr${chr}.vcf.gz --processReference --prefix m3vcfs/chr${chr} --rsid
done
```


## Create legend files

A legend file is a tab-delimited file consisting of 5 columns (`id`, `position`, `a0`, `a1`, `all.aaf`).

```sh
for chr in `seq 1 22` X
    do
    echo "id position a0 a1 all.aaf" > header
    bcftools query -f '%CHROM %POS %REF %ALT %AC %AN\n' chr${chr}.bcf |  awk -F" " 'BEGIN { OFS = " " } {print $1":"$2 " " $2 " " $3 " "$4  " "  $5/$6}' | cat header - | bgzip > chr${chr}.legend.gz &
done
```


## Reference genetic maps

The genetic maps for eagle (hg19/hg38) can be found [here](https://data.broadinstitute.org/alkesgroup/Eagle/downloads/tables).

## Integrate your new reference panel
The created folder structure must be compressed to a zip archive and can now be integrated into Michigan Imputation Server. Please go to [this page](http://imputationserver.readthedocs.io/en/latest/docker/#install-1000g-phase-3-reference-panel) for further instructions. A full working zip archive for Hapmap can be found [here](https://imputationserver.sph.umich.edu/static/downloads/releases/hapmap2-1.0.0.zip).


