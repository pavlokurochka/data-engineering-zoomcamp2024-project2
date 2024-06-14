# GithHub Actions configuration

GiotHub Actions is an alternative free orchestration. It is configured to run daily on GitHub codespace. It runs for less then 2 min per day and easily fits in the free-use codespaces limit.


## Add Repo Secrets

Notice that in the data-pipeline.yml file I referenced a variable *${{ secrets.MOTHERDUCK_TOKEN }}*.

It is one of the  **repository secrets** that are accessible to GitHub Actions as an environment variables. To create them, we go to our repository settings, click Secrets and Variables, select Actions, and click “New repository secret”.

![](pictures/repo_secret.png)

Add a value for MOTHERDUCK_TOKEN. Check main [readme](README.md) on how to get it.



## Orchestration

[Workflow](.github/workflows/data-pipeline.yml).is configured in a YAML file, similar to Kestra. It has the same functionality as **motherduck_facts** flow in Kestra.

It runs two Python scripts: `create_secrets.py` and  `run_backfill.py`. After that it runs the same SQLMesh config as Kestra worflow.

