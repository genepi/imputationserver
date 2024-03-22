# Getting started

To use Michigan Imputation Server, a [registration](https://imputationserver.sph.umich.edu/index.html#!pages/register) is required.
We send an activation mail to the provided address. Please follow the instructions in the email to activate your account. If it doesn't arrive, ensure you have entered the correct email address and check your spam folder.

**After the email address has been verified, the service can be used without any costs.**

Please cite this paper if you use Michigan Imputation Server in your GWAS study:

> Das S, Forer L, Schönherr S, Sidore C, Locke AE, Kwong A, Vrieze S, Chew EY, Levy S, McGue M, Schlessinger D, Stambolian D, Loh PR, Iacono WG, Swaroop A, Scott LJ, Cucca F, Kronenberg F, Boehnke M, Abecasis GR, Fuchsberger C. [Next-generation genotype imputation service and methods](https://www.ncbi.nlm.nih.gov/pubmed/27571263). Nature Genetics 48, 1284–1287 (2016).


## Setup your first imputation job

Please [login](https://imputationserver.sph.umich.edu/index.html#!pages/login) with your credentials and click on the **Run** tab to start a new imputation job. The submission dialog allows you to specify the properties of your imputation job.

![](images/submit-job01.png)

The following options are available:

### Reference Panel

Our server offers genotype imputation from different reference panels. The most accurate and largest panel is **HRC (Version r1.1 2016)**. Please select one that fulfills your needs and supports the population of your input data:

- HRC (Version r1.1 2016)
- HLA Imputation Panel: two-field (four-digit) and G-group resolution
- HRC (Version r1 2015)
- 1000 Genomes Phase 3 (Version 5)
- 1000 Genomes Phase 1 (Version 3)
- CAAPA - African American Panel
- HapMap 2

More details about all available reference panels can be found [here](https://imputationserver.readthedocs.io/en/latest/reference-panels/).

### Upload VCF files from your computer

When using the file upload, data is uploaded from your local file system to Michigan Imputation Server. By clicking on **Select Files** an open dialog appears where you can select your VCF files:

![](images/upload-data01.png)

Multiple files can be selected using the `ctrl`, `cmd` or `shift` keys, depending on your operating system.
After you have confirmed your choice, all selected files are listed in the submission dialog:

![](images/upload-data02.png)

Please make sure that all files fulfill the [requirements](/prepare-your-data).


!!! important
    Since version 1.7.2 URL-based uploads (sftp and http) are no longer supported. Please use direct file uploads instead. 

### Build
Please select the build of your data. Currently the options **hg19** and **hg38** are supported. Michigan Imputation Server automatically updates the genome positions (liftOver) of your data. All reference panels except TOPMed are based on hg19 coordinates.

### rsq Filter
To minimize the file size, Michigan Imputation Server includes a r<sup>2</sup> filter option, excluding all imputed SNPs with a r<sup>2</sup>-value (= imputation quality) smaller then the specified value.

### Phasing

If your uploaded data is *unphased*, Eagle v2.4 will be used for phasing. In case your uploaded VCF file already contains phased genotypes, please select the "No phasing" option.

|  Algorithm | Description |
| ---------- |-------------|
| **Eagle v2.4** | The [Eagle](https://data.broadinstitute.org/alkesgroup/Eagle/) algorithm estimates haplotype phase using the HRC reference panel. This method is also suitable for single sample imputation. After phasing or imputation you will receive phased genotypes in your VCF files. |

### Population

Please select the population of your uploaded samples. This information is used to compare the allele frequencies between your data and the reference panel. Please note that not every reference panel supports all sub-populations.

|  Population | Supported Reference Panels |
| ----------- | ---------------------------|
| **AFR** | all |
| **AMR** | all |
| **EUR** | all |
| **Mixed** | all |
| **AA** | CAAPA |
| **ASN** | 1000 Genomes Phase 1 (Version 3) |
| **EAS** | 1000 Genomes Phase 3 (Version 5) |
| **SAS** | 1000 Genomes Phase 3 (Version 5) |

In case your population is not listed or your samples are from different populations, please select **Mixed** to skip the allele frequency check. For mixed populations, no QC-Report will be created.

### Mode

Please select if you want to run **Quality Control & Imputation**, **Quality Control & Phasing Only** or **Quality Control Only**.


### AES 256 encryption

All Imputation Server results are encrypted by default. Please tick this checkbox if you want to use AES 256 encryption instead of the default encryption method. Please note that AES encryption does not work with standard unzip programs. We recommend to use 7z instead.


## Start your imputation job

After confirming our *Terms of Service*, the imputation process can be started immediately by clicking on **Start Imputation**. Input Validation and Quality Control are executed immediately to give you feedback about the data-format and its quality. If your data passed this steps, your job is added to our imputation queue and will be processed as soon as possible. You can check the position in the queue on the job summary page.

![](images/queue01.png)

We notify you by email as soon as the job is finished or your data don't pass the Quality Control steps.

### Input Validation

In a first step we check if your uploaded files are valid and we calculate some basic statistics such as amount of samples, chromosomes and SNPs.

![](images/input-validation01.png)

After Input Validation has finished, basic statistics can be viewed directly in the web interface.

![](images/input-validation02.png)

If you encounter problems with your data please read this tutorial about [Data Preparation](/prepare-your-data) to ensure your data is in the correct format.

### Quality Control

In this step we check each variant and exclude it in case of:

1. contains invalid alleles
2. duplicates
3. indels
4. monomorphic sites
5. allele mismatch between reference panel and uploaded data
6. SNP call rate < 90%

All filtered variants are listed in a file called `statistics.txt` which can be downloaded by clicking on the provided link. More informations about our QC pipeline can be found [here](/pipeline).

![](images/quality-control02.png)

If you selected a population, we compare the allele frequencies of the uploaded data with those from the reference panel. The result of this check is available in the QC report and can be downloaded by clicking on `qcreport.html`.

### Pre-phasing and Imputation

Imputation is achieved with Minimac4. The progress of all uploaded chromosomes is updated in real time and visualized with different colors.

![](images/imputation01.png)

### Data Compression and Encryption

If imputation was successful, we compress and encrypt your data and send you a random password via mail.

![](images/compression01.png)

This password is not stored on our server at any time. Therefore, if you lost the password, there is no way to resend it to you.

## Download results

The user is notified by email, as soon as the imputation job has finished. A zip archive including the results can be downloaded directly from the server. To decrypt the results, a one-time password is generated by the server and included in the email. The QC report and filter statistics can be displayed and downloaded as well.

![](images/job-results.png)

!!! important "All data is deleted automatically after 7 days"
    Be sure to download all needed data in this time period. We send you a reminder 48 hours before we delete your data. Once your job hast the state **retired**, we are not able to recover your data!


###  Download via a web browser

All results can be downloaded directly via your browser by clicking on the filename.

![](images/share-data02.png)

In order to download results via the commandline using `wget`or `aria2` you need to click on the **share** symbol (located right to the file size) to get the needed private links.

![](images/share-data01.png)

A new dialog appears which provides you the private link. Click on the tab **wget command** to get a copy & paste ready command that can be used on Linux or MacOS to download the file in you terminal:


### Download all results at once

To download all files of a folder (for example folder **Imputation Results**) you can click on the **share** symbol of the folder:

![](images/share-data02.png)

A new dialog appears which provides you all private links at once. Click on the tab **wget commands** to get copy & paste ready commands that can be used on Linux or MacOS to download all files.
