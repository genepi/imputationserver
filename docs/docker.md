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

!!! warning
    A local sudo user (not root) is required to run the image.


## Michigan Imputation Server: Quick Start

To test the overall setup, please execute the following commands to setup the Michigan Imputation Server using HapMap only.

### Setup

````sh
docker pull seppinho/cloudgene-docker
sudo docker run --privileged -it -p 8082:8082 seppinho/cloudgene-docker --repository https://github.com/genepi/imputationserver.git
````

### Connect

````
http://localhost:8082
````
-or-

````
http://your-ip-address:8082
````

**Credentials**: admin / admin1978


### Download sample data

A sample file (chromosome 1) including ~23k SNPs (simulated HapMap300 chip) can be found [here](https://imputationserver.sph.umich.edu/static/downloads/hapmap300.chr1.recode.vcf.gz).


## Michigan Imputation Server: Production Mode

For setting up Michigan Imputation Server using 1000 Genomes Phase3, please execute the following commands:

### Create a local Docker directory

````sh
mkdir /tmp/imputation-data
````

!!! warning
    All subsequent commands must be executed from this directory (i.e. /tmp/imputation-data).

### Get 1000 genomes reference data

````sh
cd /tmp/imputation-data
wget https://imputationserver.sph.umich.edu/static/downloads/1KP3.tar.gz -O 1KGP3.tar.gz
tar xvfz 1KGP3.tar.gz
````

!!! note
    It's recommended to execute all commands from a HDD having at least 100 GB (Reference panel file size zipped: 14 GB).

### Build latest source locally

````sh
cd /tmp/imputation-data
git clone https://github.com/genepi/imputationserver.git
mvn clean install -Dmaven.test.skip=true -f imputationserver/pom.xml
````

### Start Docker image

````sh
sudo docker pull seppinho/cloudgene-docker
````

````sh
cd /tmp/imputation-data
sudo docker run --privileged -it -p 8082:8082 -p 50030:50030 -p 50060:50060 -v $(pwd)/imputationserver/target/minimac-cloud-assembly:/opt/cloudgene/apps/ -v $(pwd):/opt/cloudgene/input-data seppinho/cloudgene-docker start-cloudgene.sh
````

!!! note
    The `sudo docker run` command ends with a root bash within the Docker Image.

### Import 1000 genomes reference data

````sh
sh /opt/cloudgene/apps/config/init-1KP3.sh
````

!!! warning
    This command has to be executed from the Docker root shell (e.g. `root@a4146a4ab3a3`).

### Ready to connect!

````
http://localhost:8082
````

-or-

````
http://your-ip-address:8082
````

**Credentials**: admin / admin1978


## Contact

Feel free to contact [Sebastian Schoenherr](mailto:sebastian.schoenherr@i-med.ac.at) or [Lukas Forer](mailto:lukas.forer@i-med.ac.at) in case of any problems.
