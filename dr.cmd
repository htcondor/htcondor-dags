@echo off

SET CONTAINER_TAG=htcondor-dags-test-container

docker build ^
       --quiet ^
       --tag %CONTAINER_TAG% ^
       --file tests/_inf/Dockerfile ^
       .

docker run ^
       -it ^
       --rm ^
       --mount type=bind,source="%CD%",target=/home/dagger/htcondor-dags ^
       -p 8000:8000 ^
       %CONTAINER_TAG% ^
       %*
