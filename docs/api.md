# API Reference

The REST APIs provide programmatic ways to submit new jobs and to download data from Michigan Imputation Server. It identifies users using authentication tokens, responses are provided in JSON format.


## Authentication
Michigan Imputation Server uses a token-based authentication. The token is required for all future interaction with the server. The token can be created and downloaded from your user profile (username -> Profile):

![Activate API](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/api.png)

For security reasons, Api Tokens are valid for 30 days. You can check the status in the web interface.


## Job Submission for Whole Genome Imputation
The API allows to submit imputation jobs and to set several parameters. For HLA imputation, please see below. 

### POST /jobs/submit/minimac4

The following parameters can be set:

| Parameter        | Values           | Default Value  |  Required  |
| ------------- |:-------------| :-----|---|
| files         | /path/to/file |  | **x** |
| mode          | `qconly`<br> `phasing` <br> `imputation`     | `imputation`   | |
| password      | user-defined password      |  auto generated and send by mail  | |
| refpanel      | `hrc-r1.1` <br> `1000g-phase-3-v5` <br>  `gasp-v2` <br>  `genome-asia-panel`  <br> `1000g-phase-1` <br> `cappa` <br> `hapmap-2` | - | **x** |
| phasing     | `eagle`<br> `no_phasing`      |  `eagle`  | |
| population  | `eur`<br> `afr`<br> `asn`<br> `amr`<br> `sas`<br> `eas`<br> `AA`<br> `mixed` <br> `all`   |  -  | **x** |
| build       | `hg19`<br> `hg38` | `hg19`  | |
| r2Filter    | `0` <br> `0.001` <br> `0.1` <br> `0.2` <br> `0.3` | `0`  | |

## Job Submission for HLA Imputation
The API also allows to submit imputation jobs using the HLA application. Please note, that the population parameter can be skipped here. 

### POST /jobs/submit/imputationserver-hla

The following parameters can be set:

| Parameter        | Values           | Default Value  |  Required  |
| ------------- |:-------------| :-----|---|
| files         | /path/to/file |  | **x** |
| mode          | `qconly`<br> `phasing` <br> `imputation`     | `imputation`   | |
| password      | user-defined password      |  auto generated and send by mail  | |
| refpanel      | `multiethnic-hla-panel-Ggroup` <br>  `multiethnic-hla-panel-4digit` | - | **x** |
| phasing     | `eagle`<br> `no_phasing`      |  `eagle`  | |
| build       | `hg19`<br> `hg38` | `hg19`  | |
| r2Filter    | `0` <br> `0.001` <br> `0.1` <br> `0.2` <br> `0.3` | `0`  | |


### Examples: curl

#### Submit a single file

To submit a job please change `/path-to/file.vcf.gz` to a valid vcf file and update `TOKEN` with your API Token:


Command:

```sh
TOKEN="YOUR-API-TOKEN";

curl https://imputationserver.sph.umich.edu/api/v2/jobs/submit/minimac4 \
  -H "X-Auth-Token: $TOKEN" \
  -F "files=@/path-to/file.vcf.gz" \
  -F "refpanel=1000g-phase-3-v5" \
  -F "population=eur"
```

Response:

```json
{
  "id":"job-20160504-161420",
  "message":"Your job was successfully added to the job queue.",
  "success":true
}
```

#### Submit multiple files

Submits multiple vcf files and impute against 1000 Genomes Phase 3 reference panel.

Command:

```sh
TOKEN="YOUR-API-TOKEN";

curl https://imputationserver.sph.umich.edu/api/v2/jobs/submit/minimac4 \
  -H "X-Auth-Token: $TOKEN" \
  -F "files=@/path-to/file1.vcf.gz" \
  -F "files=@/path-to/file2.vcf.gz" \
  -F "refpanel=1000g-phase-3-v5" \
  -F "population=eur"
```

Response:

```json
{
  "id":"job-20120504-155023",
  "message":"Your job was successfully added to the job queue.",
  "success":true
}
```


#### Submit file from a HTTP(S)

Submits files from https with HRC reference panel and quality control.

Command:

```sh
TOKEN="YOUR-API-TOKEN";

curl  https://imputationserver.sph.umich.edu/api/v2/jobs/submit/minimac4 \
  -H "X-Auth-Token: $TOKEN" \
  -F "files=https://imputationserver.sph.umich.edu/static/downloads/hapmap300.chr1.recode.vcf.gz" \
  -F "files-source=http" \
  -F "refpanel=hrc-r1.1" \
  -F "population=eur" \
  -F "mode=qconly"
```

Response:

```json
{
  "id":"job-20120504-155023",
  "message":"Your job was successfully added to the job queue.",
  "success":true
}
```


### Examples: Python

#### Submit single vcf file

```python
import requests
import json

# imputation server url
url = 'https://imputationserver.sph.umich.edu/api/v2'
token = 'YOUR-API-TOKEN';

# add token to header (see Authentication)
headers = {'X-Auth-Token' : token }
data = {
  'refpanel': '1000g-phase-3-v5',
  'population': 'eur'
}

# submit new job
vcf = '/path/to/genome.vcf.gz';
files = {'files' : open(vcf, 'rb')}
r = requests.post(url + "/jobs/submit/minimac4", files=files, data=data, headers=headers)
if r.status_code != 200:
  print(r.json()['message'])
  raise Exception('POST /jobs/submit/minimac4 {}'.format(r.status_code))

# print response and job id
print(r.json()['message'])
print(r.json()['id'])
```

#### Submit multiple vcf files

```python
import requests
import json

# imputation server url
url = 'https://imputationserver.sph.umich.edu/api/v2'
token = 'YOUR-API-TOKEN';

# add token to header (see Authentication)
headers = {'X-Auth-Token' : token }
data = {
  'refpanel': '1000g-phase-3-v5',
  'population': 'eur'
}

# submit new job
vcf = '/path/to/file1.vcf.gz';
vcf1 = '/path/to/file2.vcf.gz';
files = [('files', open(vcf, 'rb')), ('files', open(vcf1, 'rb'))]
r = requests.post(url + "/jobs/submit/minimac4", files=files, data=data, headers=headers)
if r.status_code != 200:
  print(r.json()['message'])
  raise Exception('POST /jobs/submit/minimac4 {}'.format(r.status_code))

# print message
print(r.json()['message'])
print(r.json()['id'])
```


## List all jobs
All running jobs can be returned as JSON objects at once.
### GET /jobs

### Examples: curl

Command:

```sh
TOKEN="YOUR-API-TOKEN";

curl -H "X-Auth-Token: $TOKEN" https://imputationserver.sph.umich.edu/api/v2/jobs
```

Response:

```json
[
  {
    "applicationId":"minimac",
    "executionTime":0,
    "id":"job-20160504-155023",
    "name":"job-20160504-155023",
    "positionInQueue":0,
    "running":false,
    "state":5
  },{
    "applicationId":"minimac",
    "executionTime":0,
    "id":"job-20160420-145809",
    "name":"job-20160420-145809",
    "positionInQueue":0,
    "running":false,
    "state":5
  },{
    "applicationId":"minimac",
    "executionTime":0,
    "id":"job-20160420-145756",
    "name":"job-20160420-145756",
    "positionInQueue":0,
    "running":false,
    "state":5
  }
]
```

### Example: Python

```python
import requests
import json

# imputation server url
url = 'https://imputationserver.sph.umich.edu/api/v2'
token = 'YOUR-API-TOKEN';

# add token to header (see authentication)
headers = {'X-Auth-Token' : token }

# get all jobs
r = requests.get(url + "/jobs", headers=headers)
if r.status_code != 200:
    raise Exception('GET /jobs/ {}'.format(r.status_code))

# print all jobs
for job in r.json():
    print('{} [{}]'.format(job['id'], job['state']))
```

## Monitor Job Status

### /jobs/{id}/status

### Example: curl

Command:

```sh
TOKEN="YOUR-API-TOKEN";

curl -H "X-Auth-Token: $TOKEN" https://imputationserver.sph.umich.edu/api/v2/jobs/job-20160504-155023/status
```

Response:

```json
{
  "application":"Michigan Imputation Server (Minimac4) 1.5.8",
  "applicationId":"minimac4",
  "deletedOn":-1,
  "endTime":1462369824173,
  "executionTime":0,
  "id":"job-20160504-155023",
  "logs":"",
  "name":"job-20160504-155023",
  "outputParams":[],
  "positionInQueue":0,
  "running":false,
  "startTime":1462369824173,
  "state":5
  ,"steps":[]
}
```

## Monitor Job Details

### /jobs/{id}

### Example: curl

```sh
TOKEN="YOUR-API-TOKEN";

curl -H "X-Auth-Token: $TOKEN" https://imputationserver.sph.umich.edu/api/v2/jobs/job-20160504-155023/
```
