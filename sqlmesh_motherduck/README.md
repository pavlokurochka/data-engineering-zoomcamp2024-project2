Run SQLMesh locally

``` bash
cd sqlmesh_motherduck/
cp config_template.yaml config.yaml
```
Edit config.yaml - replace <motherduck_token> with MotherDuck Service Token from https://app.motherduck.com/settings

``` bash
pip install "sqlmesh[web]"
sqlmesh plan --auto-apply
sqlmesh ui
```

Open http://localhost:8000/editor