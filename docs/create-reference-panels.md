# Create a Reference Panel

## Requirements

### Data

Your reference data has to be available in the VCF file format (one for each chromosome). Currently GRCh37 coordinates are required. 

### Software

- [Minimac3](https://github.com/Santy-8128/Minimac3)
- [bcftools](https://samtools.github.io/bcftools/bcftools.html)


## Folder Structure

We recommend the following folder structure:

```ansi
my-ref-panel
├── cloudgene.yaml
├── bcfs
|   ├── chr1.bcf
|   ├── ...
|   └── chr22.bcf
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

- TODO: add command

## Create m3vcf files

Create m3vcf files with Minimac3:

```sh
for CHR in `seq 1 22`
do
    Minimac3 --refHaps chr${CHR}.vcf.gz --processReference --prefix m3vcfs/chr${CHR}
done
```


## Create legend files

A legend file is a space-separated file consisting of 5 columns (`id`, `position`, `a0`, `a1`, `population.aaf`). 
`a0` and `a1` including the ref/alt alleles, `population.aaf` the alternate allele frequency. 
Please note that "population" must be substituted by the actual population (e.g. eur). This population must be finally set in the [minimac4.yaml file](https://github.com/genepi/imputationserver/blob/master/files/minimac4.yaml). A legend file can be found [here](https://github.com/genepi/imputationserver/blob/master/test-data/configs/hapmap-chr20/ref-panels/hapmap_r22.chr20.CEU.hg19_impute.legend.gz?raw=true).


## map files and other

- The eagle files for hg19 including chromosome X can be found [here](https://data.broadinstitute.org/alkesgroup/Eagle/downloads/tables/genetic_map_hg19_withX.txt.gz).