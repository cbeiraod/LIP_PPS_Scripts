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

## Scripts
- `convert_scope_data.py`: This script converts a set of binary file of data taken with the Infiniium osciloscope, each file subsequently called and associated with a run, into the data format used in the LIP PPS LGAD analysis framework
- `summarise_pulse_waveforms.py`: WIP This script fetches high level data from runs of pulses
