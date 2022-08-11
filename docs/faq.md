# Frequently Asked Questions

## I did not receive a password for my imputation job
Munich Imputation Server creates a random password for each imputation job. This password is not stored on server-side at any time. If you didn't receive a password, please check your Mail SPAM folder. Please note that we are not able to re-send you the password.  

## Unzip command is not working
Please check the following points: (1) When selecting AES256 encryption, please use 7z to unzip your files (Debian: `sudo apt-get install p7zip-full`). For our default encryption all common programs should work. (2) If your password includes special characters (e.g. \\), please put single or double quotes around the password when extracting it from the command line (e.g. `7z x -p"PASSWORD" chr_22.zip`).

## Extending expiration date or reset download counter
Your data is available for 7 days. In case you need an extension, please let [us](/contact) know.

## How can I improve the download speed?
[aria2](https://aria2.github.io/) tries to utilize your maximum download bandwidth. Please keep in mind to raise the k parameter significantly (-k, --min-split-size=SIZE). You will otherwise hit the Munich Imputation Server download limit for each file (thanks to Anthony Marcketta for point this out).

## Can I download all results at once?
We provide wget command for all results. Please open the results tab. The last column in each row includes direct links to all files.

## Can I set up Munich Imputation Server locally?
We are providing a single-node Docker image that can be used to impute from Hapmap2 and 1000G Phase3 locally. Click [here](/docker) to give it a try. For usage in production, we highly recommend setting up a Hadoop cluster.

## Your web service looks great. Can I set up my own web service as well?
All web service functionality is provided by [Cloudgene](http://www.cloudgene.io/). Please contact us, in case you want to set up your own service.
