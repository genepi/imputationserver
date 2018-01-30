# Michigan Imputation Server on Docker

This tutorial sets up the Michigan Imputation Server on [Docker](https://www.docker.com/). To impute against the [HRC](http://www.haplotype-reference-consortium.org) reference panel, please use the official instance of the [Michigan Imputation Server](https://imputationserver.sph.umich.edu).


## License

Michigan Imputation Server is licensed under [AGPL](https://www.gnu.org/licenses/agpl-3.0.html).


## Requirements

Docker must be installed on your local computer. Please checkout the [step by step guide](https://docs.docker.com/engine/installation/linux/ubuntu/) to install the latest version.

### Check Docker Version

````
docker --version
Docker version 17.03.1-ce, build c6d412e
````


## Quick Start

After the successful installation of Docker, all you need to do is:

### Setup

````sh
docker run -d -p 8080:80 genepi/imputationserver
````

After ~ 1 minute your Imputation Server instance is ready and you are able to access it on http://localhost:8080 and impute against HapMap2.

### Connect

**URL:** http://localhost:8082.

**Credentials:** admin / admin1978


### Download sample data

A sample file (chromosome 1) including ~23k SNPs (simulated HapMap300 chip) can be found [here](https://imputationserver.sph.umich.edu/static/downloads/hapmap300.chr1.recode.vcf.gz).


##  Install 1000 Genomes Phase 3 reference panel

This image is delivered with the Hapmap2 Reference Panel. To keep your jobs, results and installed reference panels between sessions, you need to mount a folder from your host to the container:

### Step 1: Setup

```sh
docker run -d -p 8080:80  -v /home/user/imputationserver-data/:/data/ genepi/imputationserver
```

**Note:** Please replace `/home/user/imputationserver-data` with the absolute path pointing to folder on your computer.

### Step 2: Login

**URL:** http://localhost:8082.

**Credentials:** admin / admin1978

### Step 3: Go to Admin Panel

After logging in, you have to open the *Admin-Panel*:

![Admin Panel](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/admin-panel.png?raw=true)

### Step 3: Open Applications

Click on the *Applications* tab to see all installed applications.

![Applications](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/applications.png?raw=true)

### Step 4: Install Application

After clicking on *Install App* a new Dialog appears, where you can enter the ID and the URL of a public available reference panel:

![Install App](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/install-app.png?raw=true)

By clicking on *OK* the installation starts. Depending on your Internet connection and computer resources it could take several minutes.

### Step 5: Submit Job

If the installation was successful, you should see your reference panel in the Reference Panel list when you submit a new job:

![Reference Panel List](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/run.png?raw=true)

Since all reference panels are installed in your provided data folder, you can stop and restart your cluster without reinstalling them.

## Public Reference Panels

Currently, the following Reference Panels are public available:

### Hapmap2

- **ID:** hapmap2
- **URL:** https://imputationserver.sph.umich.edu/static/downloads/releases/hapmap2-1.0.0.zip

### 1000 Genomes Phase 3

- **ID:** 1000genomes-phase3
- **URL:** https://imputationserver.sph.umich.edu/static/downloads/releases/1000genomes-phase3-1.0.0.zip



## Contact

Feel free to contact [Sebastian Schoenherr](mailto:sebastian.schoenherr@i-med.ac.at) or [Lukas Forer](mailto:lukas.forer@i-med.ac.at) in case of any problems.
