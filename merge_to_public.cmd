setlocal
set repo=%1
if not defined repo (
    copy /y %~dpnx0 %temp%\
    call %temp%\%~nx0 %~dp0
    goto :EOF
)

git checkout public
if %errorlevel% NEQ 0 (
    goto :EOF
)
cmd /c gitm main
git rm -f %repo%\merge_to_public.cmd
REM add more git rm -f lines here to remove other private files
