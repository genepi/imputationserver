## Release Procedure ##

### Checkout master ###

Update your repository and run all tests with 'mvn test'. If an error is raised, it needs to be fixed and committed before going to the next step.

### Update version number

Update version according Sematic Versioning:

> Given a version number MAJOR.MINOR.PATCH, increment the:
>
> - MAJOR version when you make incompatible API changes,
> - MINOR version when you add functionality in a backwards-compatible manner, and
> - PATCH version when you make backwards-compatible bug fixes.
>
> Additional labels for pre-release and build metadata are available as extensions to the MAJOR.MINOR.PATCH format.
>
> https://semver.org/


We track our current version in
- `pom.xml`
- `files/minimac.yaml`.

Set the new version without the `v` prefix.

Then, commit and push the changes:

    git commit -m 'Prepare release 1.x.x'

**This should be the last commit before the release.**

### Create the release on GitHub

 Create a new release with tag `v1.x.x` (prefix `v`) and description including the major points and the changelog. The name of the new release has to be `Michigan Imputation Server 1.x.x`.

 **Travis creates `imputationserver.zip` and uploads the file as asset to the new release.**

### Update Docker image

Update [apps.yaml](https://github.com/genepi/imputationserver-docker/blob/master/apps.yaml) with the URL pointing to the latest release on GitHub. Change the property `url` of application `imputationserver` to release 1.x.x:

```yaml
id: imputationserver
url: https://github.com/genepi/imputationserver/releases/download/1.x.x/imputationserver.zip
```

Use the following commit message: `Updated to version 1.x.x`

**Travis tests the image and DockerHub deploys it. Check status on https://github.com/genepi/imputationserver-docker**
