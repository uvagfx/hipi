@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem
@rem  jpegFromHib startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

@rem Add default JVM options here. You can also use JAVA_OPTS and JPEG_FROM_HIB_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if "%ERRORLEVEL%" == "0" goto init

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:init
@rem Get command-line arguments, handling Windowz variants

if not "%OS%" == "Windows_NT" goto win9xME_args
if "%@eval[2+2]" == "4" goto 4NT_args

:win9xME_args
@rem Slurp the command line arguments.
set CMD_LINE_ARGS=
set _SKIP=2

:win9xME_args_slurp
if "x%~1" == "x" goto execute

set CMD_LINE_ARGS=%*
goto execute

:4NT_args
@rem Get arguments from the 4NT Shell from JP Software
set CMD_LINE_ARGS=%$

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\jpegFromHib.jar;%APP_HOME%\lib\hadoop-common-2.7.1.jar;%APP_HOME%\lib\hadoop-mapreduce-client-core-2.7.1.jar;%APP_HOME%\lib\json-simple-1.1.1.jar;%APP_HOME%\lib\metadata-extractor-2.8.1.jar;%APP_HOME%\lib\imageio-jpeg-3.1.1.jar;%APP_HOME%\lib\imageio-pnm-3.1.1.jar;%APP_HOME%\lib\imageio-tiff-3.1.1.jar;%APP_HOME%\lib\javacpp-1.0.jar;%APP_HOME%\lib\javacv-1.0.jar;%APP_HOME%\lib\opencv-3.0.0-1.0.jar;%APP_HOME%\lib\opencv-3.0.0-1.0-macosx-x86_64.jar;%APP_HOME%\lib\hipi-2.1.0.jar;%APP_HOME%\lib\hadoop-annotations-2.7.1.jar;%APP_HOME%\lib\commons-cli-1.2.jar;%APP_HOME%\lib\commons-math3-3.1.1.jar;%APP_HOME%\lib\xmlenc-0.52.jar;%APP_HOME%\lib\commons-httpclient-3.1.jar;%APP_HOME%\lib\commons-io-2.4.jar;%APP_HOME%\lib\commons-net-3.1.jar;%APP_HOME%\lib\commons-collections-3.2.1.jar;%APP_HOME%\lib\servlet-api-2.5.jar;%APP_HOME%\lib\jetty-6.1.26.jar;%APP_HOME%\lib\jetty-util-6.1.26.jar;%APP_HOME%\lib\jsp-api-2.1.jar;%APP_HOME%\lib\jersey-core-1.9.jar;%APP_HOME%\lib\jersey-json-1.9.jar;%APP_HOME%\lib\jersey-server-1.9.jar;%APP_HOME%\lib\commons-logging-1.1.3.jar;%APP_HOME%\lib\log4j-1.2.17.jar;%APP_HOME%\lib\jets3t-0.9.0.jar;%APP_HOME%\lib\commons-lang-2.6.jar;%APP_HOME%\lib\commons-configuration-1.6.jar;%APP_HOME%\lib\slf4j-api-1.7.10.jar;%APP_HOME%\lib\slf4j-log4j12-1.7.10.jar;%APP_HOME%\lib\jackson-core-asl-1.9.13.jar;%APP_HOME%\lib\jackson-mapper-asl-1.9.13.jar;%APP_HOME%\lib\avro-1.7.4.jar;%APP_HOME%\lib\protobuf-java-2.5.0.jar;%APP_HOME%\lib\gson-2.2.4.jar;%APP_HOME%\lib\hadoop-auth-2.7.1.jar;%APP_HOME%\lib\jsch-0.1.42.jar;%APP_HOME%\lib\curator-client-2.7.1.jar;%APP_HOME%\lib\curator-recipes-2.7.1.jar;%APP_HOME%\lib\jsr305-3.0.0.jar;%APP_HOME%\lib\htrace-core-3.1.0-incubating.jar;%APP_HOME%\lib\zookeeper-3.4.6.jar;%APP_HOME%\lib\commons-compress-1.4.1.jar;%APP_HOME%\lib\hadoop-yarn-common-2.7.1.jar;%APP_HOME%\lib\guice-servlet-3.0.jar;%APP_HOME%\lib\junit-4.10.jar;%APP_HOME%\lib\xmpcore-5.1.2.jar;%APP_HOME%\lib\imageio-core-3.1.1.jar;%APP_HOME%\lib\imageio-metadata-3.1.1.jar;%APP_HOME%\lib\common-lang-3.1.1.jar;%APP_HOME%\lib\common-io-3.1.1.jar;%APP_HOME%\lib\common-image-3.1.1.jar;%APP_HOME%\lib\ffmpeg-2.7.1-1.0.jar;%APP_HOME%\lib\flycapture-2.7.3.19-1.0.jar;%APP_HOME%\lib\libdc1394-2.2.3-1.0.jar;%APP_HOME%\lib\libfreenect-0.5.2-1.0.jar;%APP_HOME%\lib\videoinput-0.200-1.0.jar;%APP_HOME%\lib\artoolkitplus-2.3.1-1.0.jar;%APP_HOME%\lib\flandmark-1.07-1.0.jar;%APP_HOME%\lib\jettison-1.1.jar;%APP_HOME%\lib\jaxb-impl-2.2.3-1.jar;%APP_HOME%\lib\asm-3.1.jar;%APP_HOME%\lib\java-xmlbuilder-0.4.jar;%APP_HOME%\lib\commons-digester-1.8.jar;%APP_HOME%\lib\commons-beanutils-core-1.8.0.jar;%APP_HOME%\lib\paranamer-2.3.jar;%APP_HOME%\lib\snappy-java-1.0.4.1.jar;%APP_HOME%\lib\apacheds-kerberos-codec-2.0.0-M15.jar;%APP_HOME%\lib\curator-framework-2.7.1.jar;%APP_HOME%\lib\jline-0.9.94.jar;%APP_HOME%\lib\xz-1.0.jar;%APP_HOME%\lib\hadoop-yarn-api-2.7.1.jar;%APP_HOME%\lib\jaxb-api-2.2.2.jar;%APP_HOME%\lib\jersey-client-1.9.jar;%APP_HOME%\lib\guice-3.0.jar;%APP_HOME%\lib\jersey-guice-1.9.jar;%APP_HOME%\lib\hamcrest-core-1.1.jar;%APP_HOME%\lib\commons-beanutils-1.7.0.jar;%APP_HOME%\lib\apacheds-i18n-2.0.0-M15.jar;%APP_HOME%\lib\api-asn1-api-1.0.0-M20.jar;%APP_HOME%\lib\api-util-1.0.0-M20.jar;%APP_HOME%\lib\stax-api-1.0-2.jar;%APP_HOME%\lib\activation-1.1.jar;%APP_HOME%\lib\javax.inject-1.jar;%APP_HOME%\lib\aopalliance-1.0.jar;%APP_HOME%\lib\cglib-2.2.1-v20090111.jar;%APP_HOME%\lib\httpclient-4.2.5.jar;%APP_HOME%\lib\guava-16.0.1.jar;%APP_HOME%\lib\netty-3.7.0.Final.jar;%APP_HOME%\lib\jackson-jaxrs-1.9.13.jar;%APP_HOME%\lib\jackson-xc-1.9.13.jar;%APP_HOME%\lib\httpcore-4.2.4.jar;%APP_HOME%\lib\commons-codec-1.6.jar

@rem Execute jpegFromHib
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %JPEG_FROM_HIB_OPTS%  -classpath "%CLASSPATH%" org.hipi.tools.JpegFromHib %CMD_LINE_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if "%ERRORLEVEL%"=="0" goto mainEnd

:fail
rem Set variable JPEG_FROM_HIB_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
if  not "" == "%JPEG_FROM_HIB_EXIT_CONSOLE%" exit 1
exit /b 1

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
