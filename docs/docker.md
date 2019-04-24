# Michigan Imputation Server on Docker

This tutorial starts a local instance of Michigan Imputation Server on [Docker](https://www.docker.com/). Imputation from HapMap2 and 1000 Genomes Phase3 is possible. To impute against the [HRC](http://www.haplotype-reference-consortium.org) reference panel, please use the official instance of the [Michigan Imputation Server](https://imputationserver.sph.umich.edu).


## License

Michigan Imputation Server is licensed under [AGPL](https://www.gnu.org/licenses/agpl-3.0.html).


## Requirements

Docker must be installed on your local computer. Please checkout the [step by step guide](https://docs.docker.com/engine/installation/linux/ubuntu/) to install the latest version.


## Setup Michigan Imputation Server

After the successful installation of Docker, all you need to do is:

````sh
docker run -d -p 8080:80 -e DOCKER_CORES="4" -v /home/user/imputationserver-data/:/data/ genepi/imputationserver:v1.1.15
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

First, start the Cloudgene webserver:

```sh
docker run -d -p 8080:80 -e DOCKER_CORES="4" -v /home/user/imputationserver-data/:/data/ --name mis-docker genepi/imputationserver:v1.1.15
```


### Run imputation on the command line

```sh
TEST_DATA="https://imputationserver.sph.umich.edu/static/downloads/hapmap300.chr1.recode.vcf.gz"
docker exec -t -i mis-docker cloudgene run imputationserver \
--files ${TEST_DATA} --refpanel apps@hapmap2 --conf /etc/hadoop/conf
```

### Install 1000 Genomes Phase3 using the command line
```sh
docker exec -t -i mis-docker cloudgene install 1000genomes-phase3 \
https://imputationserver.sph.umich.edu/static/downloads/releases/1000genomes-phase3-1.0.0.zip
```


## Contact

Feel free to contact [Sebastian Schoenherr](mailto:sebastian.schoenherr@i-med.ac.at) or [Lukas Forer](mailto:lukas.forer@i-med.ac.at) in case of any problems.
