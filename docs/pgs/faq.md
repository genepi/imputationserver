# Frequently Asked Questions

## Can I use the Polygenic Score Calculation extension without an email address?
Yes, the extension can also be used with a username without an email. However, without an email, notifications are not sent, and access to genotyped data may be limited.

## Extending expiration date or reset download counter
Your data is available for 7 days. In case you need an extension, please let [us](/contact) know.

## How can I improve the download speed?
[aria2](https://aria2.github.io/) tries to utilize your maximum download bandwidth. Please keep in mind to raise the k parameter significantly (-k, --min-split-size=SIZE). You will otherwise hit the Michigan Imputation Server download limit for each file (thanks to Anthony Marcketta for point this out).

## Can I download all results at once?
We provide wget command for all results. Please open the results tab. The last column in each row includes direct links to all files.

## Can I perform PGS calculation locally?
Imputationserveris using a standalone tool called pgs-calc. It reads the imputed dosages from VCF files and uses them to calculate scores. It supports imputed genotypes from Michigan Imputation Server or TOPMed Imputation Server out of the box and score files from PGS Catalog or PRSWeb instances. In addition, own created score files containing chromosomal positions, both alleles and the effect size can be used easily. pgs-calc uses the chromosomal positions and alleles to find the corresponding dosages in genotype files, but provides also tools to resolve rsIDs in score files using dbSNP. Therefore, it can be applied to genotype files with variants that were not annotated with rsIDs. Moreover, the standalone version provides options to improve the coverage by using the provided proxy mapping file for Europeans or a custom population specific mapping file. pgs-calc is available at https://github.com/lukfor/pgs-calc.