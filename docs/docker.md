# Michigan Imputation Server on Docker

This tutorial starts a local instance of Michigan Imputation Server on [Docker](https://www.docker.com/). Imputation from HapMap2 and 1000 Genomes Phase3 is possible. To impute against the [HRC](http://www.haplotype-reference-consortium.org) reference panel, please use the official instance of the [Michigan Imputation Server](https://imputationserver.sph.umich.edu).


## License

Michigan Imputation Server is licensed under [AGPL](https://www.gnu.org/licenses/agpl-3.0.html).


## Requirements

Docker must be installed on your local computer. Please checkout the [step by step guide](https://docs.docker.com/engine/installation/linux/ubuntu/) to install the latest version.


## Setup Michigan Imputation Server

After the successful installation of Docker, all you need to do is:

````sh
docker run -d -p 8080:80 -e DOCKER_CORES="4" -v /home/user/imputationserver-data/:/data/ genepi/imputationserver:v1.2.2
````
Please replace `/home/user/imputationserver-data` with the absolute path pointing to a local folder on your computer. This allows you to keep all data after a restart. To run more parallel tasks, please adapt the `DOCKER_CORES` parameter.

After ~2-3 minutes your Michigan Imputation Server instance is ready and you are able to access it on http://localhost:8080.

## Submit Job

Login as **admin** with the default admin password **admin1978**. You can now start a job by clicking on *Run*. More about submitting jobs and data preparation can be found in our [Documentation](http://imputationserver.readthedocs.io/en/latest/getting-started/#setup-your-first-imputation-job).

## Install 1000G Phase 3 Reference Panel

This image is delivered with the Hapmap2 Reference Panel. To install additional reference panels, you have to open the *Admin-Panel*:

![Admin Panel](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/admin-panel.png?raw=true)

### Open Applications

Click on the *Applications* tab to see all installed applications.

![Applications](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/applications.png?raw=true)

### Install Application

After clicking on *Install App* a new dialog appears, where you can enter the ID and the URL of a public available reference panel:

![Install App](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/install-app.png?raw=true)

ID and link can be copied from [here](#available-reference-panels). By clicking on *OK* the installation starts. Depending on your Internet connection and computer resources it could take several minutes.

### Submit Job

If the installation was successful, you should see your reference panel in the reference panel list when you submit a new job:

![Reference Panel List](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/run.png?raw=true)

Since all reference panels are installed in your provided data folder, you can stop and restart your cluster without reinstalling them.


## Available Reference Panels

### Hapmap2

- **ID:** hapmap2
- **URL:** https://imputationserver.sph.umich.edu/static/downloads/releases/hapmap2-1.0.0.zip

### 1000 Genomes Phase 3

- **ID:** 1000genomes-phase3
- **URL:** https://imputationserver.sph.umich.edu/static/downloads/releases/1000genomes-phase3-1.0.0.zip

## Command line Support

It is also possible to submit a job via the commandline instead of the webbrowser.

First, start the Cloudgene webserver and give your docker container a name (e.g. mis-docker):

```sh
docker run -d -p 8080:80 -e DOCKER_CORES="4" -v /home/user/imputationserver-data/:/data/ --name mis-docker genepi/imputationserver:v1.2.2
```

After ~2-3 minutes your Michigan Imputation Server instance is ready.

### Run imputation on the command line

To start a new job via the commandline you have to execute the following command:

```sh
docker exec -t -i mis-docker cloudgene run imputationserver \
  --files https://imputationserver.sph.umich.edu/static/downloads/hapmap300.chr1.recode.vcf.gz \
  --refpanel apps@hapmap2 \
```

Your input vcf files can be specified with the `--files` parameter. You can either use files inside your docker image (e.g. `--files /data/my.vcf.gz` uses a vcf stored in your mounted folder) or a http link. To impute more files at once, you can use a folder as input (e.g. `--files /path/vcfs`)

If your imputation was successful, you see the random password that was used to encrypt your results (please use it without the html tags. e.g. o1%bUVuBEf7Cl):

```
[OK]    Email notification is disabled. All results are encrypted with password <b>o1%bUVuBEf7Cl</b>
```

The imputed genotypes are stored in `/opt/cloudgene/<JOB-ID>/local`. You can change this location by using the `--output` parameter (e.g. `--output /data/my_first_job` stores the results in your mounted folder).

### Install reference panels on the command line

To install additional reference panels via the command line, you can use the `install` command followed by the name and the location of the reference panel. The following command installs the 1000 genomes phase 3 reference panel:

```sh
docker exec -t -i mis-docker cloudgene install 1000genomes-phase3 https://imputationserver.sph.umich.edu/static/downloads/releases/1000genomes-phase3-1.0.0.zip
```

The location of the reference panel can be either a local file or a http(s) link.

## Contact

Feel free to contact [Sebastian Schoenherr](mailto:sebastian.schoenherr@i-med.ac.at) or [Lukas Forer](mailto:lukas.forer@i-med.ac.at) in case of any problems.
