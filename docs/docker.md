# Michigan Imputation Server (MIS) on Docker

This tutorial sets up the Michigan Imputation Server on [Docker](https://www.docker.com/). To impute against the [HRC](http://www.haplotype-reference-consortium.org) reference panel, please use the official instance of the [Michigan Imputation Server](https://imputationserver.sph.umich.edu).


## License

Michigan Imputation Server is licensed under [AGPL](https://www.gnu.org/licenses/agpl-3.0.html).


## Requirements

Docker must be installed on your local computer. Please checkout the [step by step guide](https://docs.docker.com/engine/installation/linux/ubuntu/) to install the latest version.

### Check Docker Version

````
docker --version
Docker version 18.03.0-ce, build 0520e24
````

## Setup Michigan Imputation Server

```sh
docker pull genepi/imputationserver
```
````sh
docker run -d -p 8080:80 -e DOCKER_CORES="4" -v /home/user/imputationserver-data/:/data/ --name mis-docker genepi/imputationserver
````
**Note:** 
* Please replace `/home/user/imputationserver-data` with the absolute path pointing to the folder on your computer.  This allows you to restart your cluster at a later point. 
* To run more parallel tasks, please adapt the `DOCKER_CORES` parameter. 

After ~2 minutes your local Imputation Server instance is ready. 

## Execute a job graphically
Connect to the local instance with the following credentials and execute a job.

**URL:** http://localhost:8080.
**Credentials:** admin / admin1978

## Execute a job on the command line

```sh
TEST_DATA="https://imputationserver.sph.umich.edu/static/downloads/hapmap300.chr1.recode.vcf.gz"
docker exec -t -i test cloudgene run imputationserver --files ${TEST_DATA} --refpanel apps@hapmap2 --conf /etc/hadoop/conf
```

## Install 1000G Phase 3 reference panel graphically

After logging in, you have to open the *Admin-Panel*:

![Admin Panel](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/admin-panel.png?raw=true)

## Open Applications

Click on the *Applications* tab to see all installed applications.

![Applications](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/applications.png?raw=true)

## Install Application

After clicking on *Install App* a new Dialog appears, where you can enter the ID and the URL of a public available reference panel:

![Install App](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/install-app.png?raw=true)

By clicking on *OK* the installation starts. Depending on your Internet connection and computer resources it could take several minutes.

### Hapmap2

- **ID:** hapmap2
- **URL:** https://imputationserver.sph.umich.edu/static/downloads/releases/hapmap2-1.0.0.zip

### 1000 Genomes Phase 3

- **ID:** 1000genomes-phase3
- **URL:** https://imputationserver.sph.umich.edu/static/downloads/releases/1000genomes-phase3-1.0.0.zip

## Submit Job

If the installation was successful, you should see your reference panel in the Reference Panel list when you submit a new job:

![Reference Panel List](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/run.png?raw=true)

Since all reference panels are installed in your provided data folder, you can stop and restart your cluster without reinstalling them.


## Install 1000 Genomes Phase 3 reference panel on the command line
```sh
docker exec -t -i mis-docker cloudgene install 1000genomes-phase3 https://imputationserver.sph.umich.edu/static/downloads/releases/1000genomes-phase3-1.0.0.zip
```

## Contact

Feel free to contact [Sebastian Schoenherr](mailto:sebastian.schoenherr@i-med.ac.at) or [Lukas Forer](mailto:lukas.forer@i-med.ac.at) in case of any problems.
