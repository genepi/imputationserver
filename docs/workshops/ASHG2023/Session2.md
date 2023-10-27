**Workshop ASHG2023 > Session 2: Run a job, Data Preparation and Data Download**

# Welcome

Welcome to Session 2! In this session you will learn how to submit a job on Michigan Imputation Server (MIS) and how to prepare your input data that they are passing the QC step.

# Tutorial

## Getting Started
As a quick start, the following video includes all required steps to submit and monitor a job using the graphical web interface.

<iframe width="560" height="315" src="https://www.youtube.com/embed/HMP76DpL_I0?si=GSFsFMbqJQFVUhJj" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>


## Run a job on your own
After you [registered](https://imputationserver.sph.umich.edu/start.html#!pages/register) successfully, the following URL will bring you directly to the job submission page:
[https://imputationserver.sph.umich.edu/index.html#!run/minimac4](https://imputationserver.sph.umich.edu/index.html#!run/minimac4)

## Submission Page - Select parameters
The UI includes several parameters which need to be specified. Our [Getting Started guide](https://imputationserver.readthedocs.io/en/latest/getting-started/) describes all required parameters to do so.

## Submission Page - Upload data

We are providing two data datasets that can be downloaded from below. In case the unphased dataset is selected, an additional phasing step using Eagle is automatically performed. For this demo, we recommend selecting the **HapMap 2 panel** (Input parameter 'Reference panel') to get your results as quick as possible. Please also have a look at our [supported reference panels](https://imputationserver.readthedocs.io/en/latest/reference-panels/) when using MIS in a production setup.

- [Phased dataset chr20 hg19](https://github.com/genepi/imputationserver-ashg/raw/main/files/chr20.R50.merged.1.330k.recode.small.vcf.gz)
- [Unphased dataset chr20 hg19](https://github.com/genepi/imputationserver-ashg/raw/main/files/chr20.R50.merged.1.330k.recode.unphased.small.vcf.gz)

## Submission Page - Submit
After all parameters have been selected and you click 'submit', the job will be added to our Input Validation and QC queue. Please have a look at our [Data Preparation Guide](https://imputationserver.readthedocs.io/en/latest/prepare-your-data) to learn how to prepare your dataset using a pre-imputation tool.

## Monitor Jobs, Download Data
If the job passes the QC step, it will be added to our long-time queue. As soon as your job is finished, you will receive an email with the password to download and encrypt your data. Also checkout our [Pipeline Overview Guide](https://imputationserver.readthedocs.io/en/latest/pipeline/) to learn more about the different internal parameters. The complete source code can also be found on [GitHub](https://imputationserver.sph.umich.edu).

## QC Results 
A simple QC report including the frequency plot is available [here](https://htmlpreview.github.io/?https://github.com/genepi/imputationserver-ashg/blob/main/files/qcreport.html).


# Contact
If you have any questions please write [me an email](mailto:sebastian.schoenherr@i-med.ac.at) or contact me on [Twitter](https://twitter.com/seppinho).
