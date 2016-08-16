#!/bin/bash
# Install HapMap2 Panel
wget https://imputationserver.sph.umich.edu/static/downloads/hapmap.tar.gz -O /opt/cloudgene/tmp/hapmap.tar.gz
cd /opt/cloudgene/tmp; tar xvfz hapmap.tar.gz
sudo -u cloudgene hadoop fs -mkdir ref-panels
sudo -u cloudgene hadoop fs -mkdir ref-panels/hapmap_r22.eagle
sudo -u cloudgene hadoop fs -put /opt/cloudgene/tmp/bcfs/*.bcf* ref-panels/hapmap_r22.eagle/.
sudo -u cloudgene hadoop fs -put /opt/cloudgene/tmp/panel/hapmap_r22.CEU.hg19.tgz ref-panels/.
sudo -u cloudgene hadoop fs -put /opt/cloudgene/tmp/panel/hapmap_r22.CEU.hg19.legends.tgz ref-panels/.
sudo -u cloudgene hadoop fs -put /opt/cloudgene/tmp/map/genetic_map_hg19_withX.txt.gz ref-panels/.

# Connect Michigan Imputation Server with Cloudgene
mv /opt/cloudgene/applications/imputationserver/files/config/settings.yaml -R /opt/cloudgene/config/settings.yaml
mv /opt/cloudgene/applications/imputationserver/target/minimac-cloud-assembly/minimac /opt/cloudgene/applications/.
mv /opt/cloudgene/applications/imputationserver/target/minimac-cloud-assembly/pages /opt/cloudgene/.