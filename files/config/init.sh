

wget cloudgene.uibk.ac.at/downloads/hapmap.tar.gz -O /opt/cloudgene/tmp/hapmap.tar.gz
cd /opt/cloudgene/tmp; tar xvfz hapmap.tar.gz
su - cloudgene
hadoop fs -mkdir ref-panels
hadoop fs -mkdir ref-panels/hapmap_r22.eagle
hadoop fs -put /opt/cloudgene/tmp/hapmap/bcfs/*.bcf* ref-panels/hapmap_r22.eagle/.
hadoop fs -put /opt/cloudgene/tmp/hapmap/panel/hapmap_r22.CEU.hg19.tgz  ref-panels/.
hadoop fs -put /opt/cloudgene/tmp/hapmap/panel/hapmap_r22.CEU.hg19.legends.tgz  ref-panels/.
hadoop fs -put /opt/cloudgene/tmp/hapmap/map/genetic_map_hg19_withX.txt.gz  ref-panels/.