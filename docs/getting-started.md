# Getting started

To use Michigan Imputation Server, a [registration](https://imputationserver.sph.umich.edu/index.html#!pages/register) is required.
We send an activation mail to the provided address. Please follow the instructions on the email to activate your account. If it doesn't arrive, ensure you have entered the correct email and check your spam folder.

**After the email address has been verified, the service can be used without any costs.**

Please cite this paper if you use Michigan Imputation Server in your GWAS study:

> Das S, Forer L, Schönherr S, Sidore C, Locke AE, Kwong A, Vrieze S, Chew EY, Levy S, McGue M, Schlessinger D, Stambolian D, Loh PR, Iacono WG, Swaroop A, Scott LJ, Cucca F, Kronenberg F, Boehnke M, Abecasis GR, Fuchsberger C. [Next-generation genotype imputation service and methods](https://www.ncbi.nlm.nih.gov/pubmed/27571263). Nature Genetics 48, 1284–1287 (2016).


## Setup your first imputation job

Please [login](https://imputationserver.sph.umich.edu/index.html#!pages/login) with your credentials and click on the **Run** tab to start a new imputation job. The submission dialog appears where you can specify the properties of your imputation job.

![](images/image02.png)


### Reference Panel

Our server offers genotype imputation from different reference panels. The most accurate and largest panel is **HRC (Version r1.1 2016)**. Please select one that fulfils your needs and supports the population of your input data:

- TOPMed Freeze5
- HRC (Version r1.1 2016)
- HRC (Version r1 2015)
- 1000 Genomes Phase 3 (Version 5)
- 1000 Genomes Phase 1 (Version 3)
- CAAPA - African American Panel
- HapMap 2

More details about all available reference panels can be found [here](/reference-panels/).

### Input files

VCF files can be uploaded either from your local disk or by specifying a remote sftp location. In both cases, several files or several locations can be selected at once. Please make sure that all files fulfil the [requirements](/prepare-your-data).


#### Upload VCF files from your computer

When using the file upload, data is uploaded from your local file system to Michigan Imputation Server. By clicking on **Select Files** an open dialog appears where you can select your VCF files:

![](images/fileupload01.png)

Multiple files can be selected using the `ctrl`, `cmd` or `shift` keys, depending on your Operating System.
After you confirmed your choice, all selected files are listed in the submission dialog:

![](images/fileupload02.png)



#### Import VCF files from SFTP Server

An other convenient way to use your data is by importing it directly from a remote SSH server. This can be achieved by selecting **Secure File Transfer Protocol**.

![](images/image03.png)

After clicking on the **Add Files** button, a new dialog appears where you can enter your SFTP credentials and the URLs of your files or folders. A URL consists of the server address followed by the full Unix path. A path can be either point to a folder or to a file. Several paths can be specified in consecutive lines.

In this examples, the server address is `sftp://my-server.com` followed by the full path of the folder `/home/lforer/mystudy/` which contains all VCF files. No colon is included between server path and file path.

![](images/sftp01.png)

It is also possible to specify only a subset of VCF files:

![](images/sftp02.png)


!!! important
    Be sure that your SFTP server is **not in a private network** and you are able to establish a connection from outside.

### Phasing

If your uploaded data is *unphased*, then you can decide which algorithm is used to phase your data before we can start genotype imputation. Eagle v2.3 is the default phasing method. In case your uploaded VCF file contains phased genotypes, this selection has no effect and genotypes are used as uploaded.

|  Algorithm | Description |
| ---------- |-------------|
| **Eagle v2.3** | The [Eagle](https://data.broadinstitute.org/alkesgroup/Eagle/) algorithm estimates haplotype phase using the selected reference panel. This method is suitable for single sample imputation. After imputation you will receive phased genotypes in your VCF files. |
| **ShapeIT v2.r790** | The [ShapeIT](https://mathgen.stats.ox.ac.uk/genetics_software/shapeit/shapeit.html) algorithm estimates haplotype phase within your uploaded data. Your uploaded data needs min. 50 samples to use this method. After imputation you will receive unphased genotypes in your VCF files. |
| **HapiUR** | The [HapiUR](https://code.google.com/p/hapi-ur/) algorithm estimates haplotype phase within your uploaded data. Your uploaded data needs min. 50 samples to use this method. After imputation you will receive phased genotypes in your VCF files. |

If you plan to impute a single sample (e.g. from 23andMe) then you have to use **Eagle**. All other algorithms are not using a reference based approach and need more than 50 samples.

### Population

Please select the population of your uploaded samples. This information is used to compare the allele frequencies between your data and the reference panel. Please note that not every reference panel supports all subpopulations.

|  Population | Supported Reference Panels |
| ----------- | ---------------------------|
| **AFR** | all |
| **AMR** | all |
| **EUR** | all |
| **Mixed** | all |
| **AA** | CAPPA |
| **ASN** | 1000 Genomes Phase 1 (Version 3) |
| **EAS** | 1000 Genomes Phase 3 (Version 5) |
| **SAS** | 1000 Genomes Phase 3 (Version 5) |

In case your population is not listed or your samples are from different populations, please select **Mixed** to skip the allele frequency check.

### Mode

Please select if you want to run **Quality Control & Imputation** or **Quality Control Only**.


### AES 256 encryption

Imputation Server encrypts all zip files by default. Please click on the checkbox to use AES 256 encryption. Please note that AES encryption does not work with standard unzip programs. We recommender to use 7z instead.



## Start your imputation job

After confirming our *Terms of Service*, the imputation process can be started immediately by clicking on **Start Imputation**. Input Validation and Quality Control are executed immediately to give you feedback about the data-format and its quality. If your data passed this steps, your job is added to our Imputation queue and will be processed as soon as possible. You can check the position in the queue on the job summary page.

![](images/queue.png)

We notify you by email as soon as the job is finished or your data don't pass the Quality Control steps.

### Input Validation

In a first step we check if your uploaded files are valid and we calculate some basics statistics such as #samples, chromosomes and SNPs.

![](images/input-validation.png)

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

![](images/qc.png)

If you selected a population, we compare the allele frequencies of the uploaded data with those from the reference panel. The result of this check is available in the QC report and can be downloaded by clicking on `qcreport.html`.

### Pre-phasing and Imputation

Imputation is achieved with Minimac4. The progress of all uploaded chromosomes is updated in real time and visualized with different colors.

![](images/results.png)

### Data Compression and Encryption

If imputation was successfull, we compress and encrypt your data and send you a random password via mail.

![](images/export.png)

This password is not stored on our server. Therefore, if you lost the password, there is no way to resend it to you.

## Download results

The user is notified by email, as soon as the imputation job has finished. A zip archive including the results can be downloaded directly from the server. To decrypt the results, a one-time password is generated by the server and included in the email. The QC report can be displayed and downloaded as well.

![](images/downloads01.png)

!!! important "All data is deleted automatically after 3 day"
    Be sure to download all needed data in this time period. We send you a reminder 24 hours before we delete your data. Once your job hast the state **retired**, we are not able to recover your data!



###  Via web Browser

All results can be downloaded directly via your browser by clicking on the filename.

![](images/downloads02.png)

### Via Commandline

In order to download results via the commandline using `wget`or `aria2` you need to click on the **share** symbol to get the needed private links.

![](images/downloads03.png)

A new dialog appears which provides you the private link. Click on the tab **wget command** to get a copy & paste ready command that can be used on Linux or MacOS to download the file in you terminal:

![](images/share01.png)

### Download all results at once

To download all files of a folder (for example folder **Imputation Results**) you can click on the **share** symbol of the folder:

![](images/downloads04.png)

A new dialog appears which provides you all private links at once. Click on the tab  **wget commands** to get copy & paste ready commands that can be used on Linux or MacOS to download all files:

![](images/share02.png)
