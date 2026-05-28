Set-PSDebug -Trace 1
python .\create_secrets.py

Set-Location sqlmesh_motherduck
# Remove-Item db.db # Remove the db.db file to ensure a fresh start for the migration. 


sqlmesh migrate
sqlmesh plan --auto-apply

Set-Location ..
Set-PSDebug -Trace 0