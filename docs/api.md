# API Reference

The REST APIs provide programmatic ways to submit new jobs and to download data from Michigan Imputation Server. It identifies users using authentication tokens, responses are provided in JSON format.


## Authentication
Michigan Imputation Server uses a token-based authentication. The token is required for all future interaction with the server. The token can be created and downloaded from your user profile (username -> Profile):

![Activate API](https://raw.githubusercontent.com/genepi/imputationserver-docker/master/images/api.png)

## Job Submission
The API allows to set several imputation parameters.

### POST /jobs/submit/minimac4

The following parameters can be set:

| Parameter        | Values           | Default Value  |
| ------------- |:-------------| :-----|
| input-files      | /path/to/file |  |
| input-mode | qconly, imputation     | imputation   |
| input-password | user-defined password      |  auto  |
| input-files-source | file-upload, sftp, http     |  default: file-upload  |
| input-refpanel     | apps@hapmap-2, apps@hrc-r1.1, apps@1000g-phase-1, apps@1000g-phase-3-v5, apps@genome-asia-panel@1.0.0, apps@cappa   | - |
| input-phasing | eagle, no_phasing      |  eagle  |
| input-population | eur, afr, asn, amr, sas, eas, AA, mixed      |  eur  |
| input-build | hg19, hg38 | hg19  |
| input-r2Filter | 0, 0.001, 0.1, 0.2, 0.3 | 0  |


### Examples

#### Submit a single file using 1000 Genomes Phase 3

To submit a job please change `/path-to-file` to the actual path.


```sh
curl -H "X-Auth-Token: <your-API-token>" -F "input-files=@/path-to-file" -F "input-refpanel=apps@1000g-phase-3-v5" -F "input-phasing=eagle" https://imputationserver.sph.umich.edu/api/v2/jobs/submit/minimac4
```
```json
{
  "id":"job-20160504-155023",
  "message":"Your job was successfully added to the job queue.",
  "success":true
}
```

#### Submit multiple files using 1000 Genomes Phase 3

```sh
curl -H "X-Auth-Token: <your-API-token>" -F "input-files-upload=@/path-to-file1" -F "input-files-upload=@/path-to-file2" -F "input-refpanel=apps@1000g-phase-3-v5" -F "input-phasing=eagle" https://imputationserver.sph.umich.edu/api/v2/jobs/submit/minimac4
```

#### Submit file from a HTTP(S) location using HRC (QC Only!)

```sh
curl -H "X-Auth-Token: <your-API-token>" -F "input-files=https://imputationserver.sph.umich.edu/static/downloads/hapmap300.chr1.recode.vcf.gz" -F "input-files-source=http" -F "input-mode=qc" -F "input-mode=imputation" -F "input-refpanel=apps@hrc-r1.1" https://imputationserver.sph.umich.edu/api/v2/jobs/submit/minimac4
```

#### Python

```python
import requests
import json

# imputation server url
url = 'https://imputationserver.sph.umich.edu/api/v2'

# add token to header (see Authentication)
headers = {'X-Auth-Token' : token }

# submit new job
vcf = '/path/to/genome.vcf.gz';
files = {'input-files' : open(vcf, 'rb')}
r = requests.post(url + "/jobs/submit/minimac4", files=files, headers=headers)
if r.status_code != 200:
    raise Exception('POST /jobs/submit/minimac4 {}'.format(r.status_code))

# print message
print r.json()['message']
print r.json()['id']
```

## List all jobs
All running jobs can be returned as JSON objects at once.
### GET /jobs

### Examples
#### curl

```sh
curl -H "X-Auth-Token: <your-API-token>" https://imputationserver.sph.umich.edu/api/v2/jobs
```

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

#### Python

```python
import requests
import json

# imputation server url
url = 'https://imputationserver.sph.umich.edu/api/v2'

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

### Examples
#### curl

```sh
curl -H "X-Auth-Token: <your-API-token>" https://imputationserver.sph.umich.edu/api/v2/jobs/job-20160504-155023/status
```

```json
{
  "application":"Michigan Imputation Server (Minimac4) 1.1.4",
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

### Examples

#### curl
curl -H "X-Auth-Token: <your-API-token>" https://imputationserver.sph.umich.edu/api/v2/jobs/job-20160504-155023/
