# Create a Reference Panel

## Requirements

### Data

Your reference data has to be available in vcf files (one for each chromosome).

### Software

- Minimac3
- bcftools
- legend file creator (see imputationserver on github)


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

- TODO: add command and add link to legend-file creator


## map files and other

- TODO: add link to eagle map files