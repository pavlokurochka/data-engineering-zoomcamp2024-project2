@setlocal enabledelayedexpansion

set "inputFile=.env"
set "outputFile=.env_encoded"

for /f "tokens=2 delims==" %%a in (%inputFile%) do (
  set "key=%%a"
  set "value=!key!"!=%b!
  set "encodedValue=!value!"
  set "encodedValue=!encodedValue:~0,-1!" (remove trailing newline)

  echo. >> !outputFile!
  echo !key!:SET SECRET_!key!=!encodedValue! >> !outputFile!
)

echo Encoded secrets written to: %outputFile%
endlocal