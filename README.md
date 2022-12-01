# sqlfuzz

Currently uploaded to test index on pypi. To install:
`pip install -i https://test.pypi.org/simple/ sqlfuzz`

## Environment Setup

- Install postgresql-12 (https://www.postgresql.org/download/)
- Use your own data or to run the demo, download demo data from [dropbox](https://www.dropbox.com/s/jw5719fycsxq5n6/data.zip?dl=0) and unzip it to `data` folder
- Start postgresql server with data folder: `pg_ctl -D ./data start`
  - If you install postgresql-12 from apt, stop it via `sudo systemctl stop postgresql.service` and give permission to the lock file via `sudo chmod o+w /var/run/postgresql`

## Quick Start
To run sqlfuzz, simply do
```python
from sqlfuzz import test_driver

if __name__ == '__main__':
  fuzz_args = test_driver.FuzzArgs(
      output={OUTPUT_DIR},
      dbconf={DB_CONFIG_PATH},
  )
  test_driver.main(fuzz_args)
```

A sample config file looks like:
```json
{"name": "demo100", "tables": ["bonus", "emp", "dept"], "max_table": 3, "max_column": 16}
```

## Note
- grant appropriate permission to your user to access the database. ref: https://stackoverflow.com/a/23934693/10180666
- On MacOS, do `brew install coreutils` to install timeout
