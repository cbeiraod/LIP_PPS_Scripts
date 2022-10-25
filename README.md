# LIP PPS Scripts
All the scripts for running/processing the LIP PPS LGAD data/runs

## Instructions

Libraries and code tested on and written for Python3.
If you try python2, you are doing so at your own risk.

Create a venv: `python3 -m venv venv`

Activate the venv: `. venv/bin/activate`

Install the dependencies (first is probably not needed):
```shell
python -m pip install pathlib
python -m pip install plotly
python -m pip install numpy
python -m pip install pandas
python -m pip install lip-pps-run-manager
python -m pip install tqdm
```

You are now ready to run the scripts.

Once done, remember to deactivate the environment: `deactivate`

On subsequent sessions, simply activate the venv and deactivate once done, there is no need to repeat the other steps.

### LXPlus
On lxplus, the correct version of python needs to be activated.

To list available software collections: `scl -l`

To activate a specific one: `scl enable [collection] bash`

### lxbatch

Useful links:
- https://batchdocs.web.cern.ch/
- https://twiki.cern.ch/twiki/bin/view/ABPComputing/LxBatch - The links mostly point to the url above
- https://twiki.cern.ch/twiki/bin/view/ABPComputing/LxbatchHTCondor
- https://twiki.cern.ch/twiki/pub/LCG/WhiteAreas/lsf.pdf - Instructions for LSF (LSF decommissioned?)

#### HTCondor

- `condor_q`: To list jobs in queues?
- `condor_userprio`: to list user priorities
- `condor_submit`: to submit jobs with a submit file
- `condor_wait`: To monitor a job without repeated calls to `condor_q`, eg: `condor_wait -status log/hello.10934689.log`


## Scripts
- `convert_scope_data.py`: This script converts a set of binary file of data taken with the Infiniium osciloscope, each file subsequently called and associated with a run, into the data format used in the LIP PPS LGAD analysis framework
- `summarise_pulse_waveforms.py`: WIP This script fetches high level data from runs of pulses
- `convert_csv_to_sqlite.py`: This scripts converts a csv file with measurement data into an sqlite file, which is used by default in the other scripts
