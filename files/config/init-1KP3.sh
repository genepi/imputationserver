#!/bin/bash

# Install 1000GP3 Panel
# Mount local files to /opt/cloudgene/input-data 
sudo -u cloudgene  hadoop fs -mkdir ref-panels
sudo -u cloudgene  hadoop fs -mkdir ref-panels/1KP3.eagle
sudo -u cloudgene  hadoop fs -put /opt/cloudgene/input-data/bcfs/* ref-panels/1KP3.eagle/.
sudo -u cloudgene  hadoop fs -put /opt/cloudgene/input-data/panel/G1K_P3_M3VCF_FILES_WITH_ESTIMATES.tar.gz  ref-panels/.
sudo -u cloudgene  hadoop fs -put /opt/cloudgene/input-data/panel/ALL_1000G_phase3_integrated_v5.legend.tgz  ref-panels/.
sudo -u cloudgene  hadoop fs -put /opt/cloudgene/input-data/map/genetic_map_hg19_withX.txt.gz  ref-panels/.

# Set 1000GP3 
cp /opt/cloudgene/input-data/panels.txt /opt/cloudgene/apps/minimac/.
cp /opt/cloudgene/input-data/minimac-vcf.yaml /opt/cloudgene/apps/minimac/.
cp /opt/cloudgene/input-data/genetic-maps.txt /opt/cloudgene/apps/minimac/.

#set help pages
ln -s /opt/cloudgene/apps/pages pages

# Restart to register pages
cd /opt/cloudgene; sudo ./cloudgene -u cloudgene -p 8082 -a stop
cd /opt/cloudgene; sudo ./cloudgene -u cloudgene -p 8082 -a start -j /usr/lib/jvm/java-8-oracle
