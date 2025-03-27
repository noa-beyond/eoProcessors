## OpenEO Processor

This cli processor makes use of the openEO python library, for executing and managing batch EO jobs in a provider.
https://openeo.org/

### Usage
The default provider for this processor is  "https://openeo.dataspace.copernicus.eu"
and the `OPENEO_CLIENT_ID` and `OPENEO_CLIENT_SECRET` must be defined as environmental variables.

### EO Batch Commands
- cfmc - Cloud Free Monthly Composites

### EO Batch job management
- job-list
- job-info [`job-id`]
- job-delete [`job-id`]
- job-get-assets [`job-id`]
- job-assets-info [`job-id`] (mainly download url of asset)

Please execute `python cli.py --help` on further info on usage.
