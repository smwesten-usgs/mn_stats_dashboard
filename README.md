# mn_stats_dashboard
Generalize, proof-of-concept Holoviews and panel dashboard for exploring statistical summaries of CMIP6-driven SWB model output.

Create a Python environment before running the script:
```shell
mamba env create -f environment.yml
```

Activate the environment you created, then start up the dashboard:
```shell
panel serve python/interactive_parquet_file_exploration_MEAN_ANNUAL.py --show
```

