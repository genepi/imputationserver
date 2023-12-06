**Workshop ASHG2023 > Session 4: nf-gwas, Imputation Bot and PGS Server**

# nf-gwas Report
You can download a nf-gwas test report from [here](https://github.com/genepi/imputationserver-ashg/raw/main/files/nf-gwas-example.zip). Please unzip the file and open the index.html file.

# Imputation Bot Tutorial

## Requirements

You will need the following things properly installed on your computer.

* [Java 8 or higher](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)


## Download and Install

Download and install the latest version from our download page using the following commands:

```
curl -sL imputationbot.now.sh | bash
```


Test the installation with the following command:

```sh
imputationbot version
```

The documentation is available at [http://imputationbot.readthedocs.io](http://imputationbot.readthedocs.io).


## Get your API Token

Enable API access from your Profile page.

1. Login and click on your **username** and then **profile**:

![Image1](/workshops/ASHG2020/images/token1.png)

2. Click on **Create API Token**

![Image1](/workshops/ASHG2020/images/token2.png)

3. Copy your API Token and paste it when `imputationbot add-instance` ask for it.

![Image1](/workshops/ASHG2020/images/token3.png)

Api Tokens are valid for 30 days. You can check the status in the web interface or with `imputationbot instances`

![Image1](/workshops/ASHG2020/images/token4.png)

4. Next, configure imputationbot with the following command:

```
imputationbot add-instance
```

```
Imputation Bot 0.8.3 🤖
https://imputationserver.sph.umich.edu
(c) 2019-2020 Lukas Forer, Sebastian Schoenherr and Christian Fuchsberger
Built by lukas on 2020-09-01T11:31:10Z

Imputationserver Url [https://imputationserver.sph.umich.edu]:
API Token [None]: eyJjdHkiOiJ0ZXh0XC9wbGFpbiIsImFsZyI6IkhTMjU2In0.eyJtYWlsIjoibHVrYXMuZm9yZXJAaS1tZWQuYWMuYXQiLCJleHBpcmUiOjE1NzMyMjkwNTY3NTEsIm5hbWUiOiJMdWthcyBGb3JlciIsImFwaSI6dHJ1ZSwidXNlcm5hbWUiOiJsdWtmb3IifQ.qY7iEM6ul-gJ0EuHmEUHRnoS5hZs7kD1HC95NFaxE9w
```

## Run imputation

You can use the `impute` command to submit a job:

- The `--files` parameter defines the location of our VCF file. If we plan to impute more than one file we can enter the path to a folder or separate multiple filenames by `,`.
- We can use the `--refpanel` parameter to specify the reference panel. For the **1000 Geneoms Phase 3** panel we use `1000g-phase-3-v5`. If we are not sure what panels are provided by the server, we can use `imputationbot refpanels` to get a list of all reference panels and their supported populations.
- For `--population` we use `eur` which stands for **European**

The complete command looks like this:

```bash
imputationbot impute --files /path/to/your.vcf.gz --refpanel 1000g-phase-3-v5 --population eur
```

After submission we get the URL where we can monitor the progress of our job.

## Monitor Jobs

However, we can also use Imputation Bot to get a list all our jobs and their status:

```
imputationbot jobs
```

To get more details about our job, we can use the `jobs` command followed by the job ID:

```
imputationbot jobs job-XXXXXXXX-XXXXXX-XXX
```

## Download all Results

We can use the `download` command to download all imputed genotypes and the QC report at once:

```
imputationbot download job-XXXXXXXX-XXXXXX-XXX
```

If the job is still running, Imputation Bot waits until the job is finished and starts automatically with the download.

You can provide Imputation Bot the password we sent you via email and it decrypts all files for you:

```
imputationbot download job-XXXXXXXX-XXXXXX-XXX --password MYPASSWORD
```

## Documentation

The documentation is available at [http://imputationbot.readthedocs.io](http://imputationbot.readthedocs.io).

## Contact

Feel free to contact [us](https://imputationserver.sph.umich.edu/index.html#!pages/contact) in case of any problems.
