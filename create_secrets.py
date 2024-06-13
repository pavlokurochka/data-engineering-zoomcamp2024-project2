"""Create files with secretsfor github actions .github\workflows\data-pipeline.yml """
#%%
import os
#%%
contents = f"""[destination.motherduck.credentials]
database = "coh3"  
password = "{os.environ['MOTHERDUCK_TOKEN'].strip()}" """ 
with open(os.path.join('dlt_motherduck','.dlt','secrets.toml'), 'w', encoding='utf-8') as f:
    f.write(contents)
#%%
with open(os.path.join('sqlmesh_motherduck','config_template.yaml'), "r", encoding="utf-8") as file:
    contents = file.read().replace('<motherduck_token>', os.environ['MOTHERDUCK_TOKEN'].strip())
with open(os.path.join('sqlmesh_motherduck','config.yaml'), 'w', encoding='utf-8') as file:
    file.write(contents)    