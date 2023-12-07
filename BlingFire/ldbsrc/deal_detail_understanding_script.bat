@REM **************************************
@REM Change the following paths before run
@REM
@REM SET_ENV_CMD_DIR:       path to set_env.cmd
@REM
@REM SG_LDBSRC_DIR:         path to local ldbsrc. Like SearchGold\deploy\builds\data\IndexGenData\ldbsrc
@REM
@REM
@REM **************************************
  
SETLOCAL
  
@echo off
set SET_ENV_CMD_DIR=F:\Deals\Msfsacompile.Library\scripts
set SG_LDBSRC_DIR=F:\Deals\IndexGenData\ldbsrc

set PATH=%PATH%;F:\Deals\Msfsacompile.Library\scripts;F:\Deals\Msfsacompile.Library\bin\retail\amd64;
  
@echo.
@echo.
@echo SET_ENV_CMD_DIR=%SET_ENV_CMD_DIR%
@echo SG_LDBSRC_DIR=%SG_LDBSRC_DIR%
@echo.
@echo.
@echo Verify the paths above before continuation
@echo.
pause
@echo on
  
pause
@echo on
  
call %SET_ENV_CMD_DIR%\set_env.cmd
  
@echo off
@echo.
@echo.
@echo on
  
pushd %SG_LDBSRC_DIR%
  
@echo off
@echo.
@echo.
@echo on
  
rmdir /S /Q  ldb\deal\deal_detail_understanding
  
@echo off
@echo.
@echo.
@echo on
  
mkdir ldb\deal\deal_detail_understanding
 
@echo off
@echo.
@echo.
@echo on
  
@echo off
@echo.
pause
@echo.
@echo.
@echo on

nmake -f Makefile.nmake lang=deal\deal_detail_understanding mode=morph clean
nmake -f Makefile.nmake lang=deal\deal_detail_understanding clean

nmake -f Makefile.nmake lang=deal\deal_detail_understanding mode=morph all
nmake -f Makefile.nmake lang=deal\deal_detail_understanding all
  
popd
  
GOTO End
  
:Syntax
ECHO.
ECHO Usage: exe "SG_DIR"
:End