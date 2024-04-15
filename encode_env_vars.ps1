Get-Content -Path .env | ForEach-Object {
  $parts = $_ -split '='
  $key = $parts[0]
  $value = $parts[1]
  [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($value)) | Out-File -Append -FilePath .env_encoded -Encoding UTF8 -NoNewline
  Write-Output "SECRET_$key=$([System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($value)))"
} 
# | Out-File -FilePath .env_encoded