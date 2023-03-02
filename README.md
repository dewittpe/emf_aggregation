# EMF and IAMC Aggregation of Scout Results

To generate the needed output files you need to do the following

1. Ensure you have the needed python modules as defined in the conda environment
   file `environment.yml`
2. Update the `config.yml` file as needed.
  - Define the scout version,
  - Define the baseline(s) by providing the scenario name(s) and file path(s)
  - Define the ECM result(s) by provideing scenario name(s) and file path(s)
  - Define the name of the output directory.

3. Run `main.py` via

```sh
python main.py
```

Get help with the possible commands for the module:
```sh
python main.py -h
```

If you want verbose output while the code runs:
```sh
python main.py --verbose
```

If you want to specify a specific configuration file
```sh
python main.py --config <path>
```

The `emf_output_dir` specified in the `config.yml` will be created, if needed,
by `main.py`.

## TODOs:
The abbility to specify specific conversion factors for CO2 by specifying a file
path is hinted at in `config.yml` but is not currently implimented.
