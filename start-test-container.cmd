@echo off

SET CONTAINER_TAG=htcondor-dags-test-container

docker build -t %CONTAINER_TAG% -f tests/_inf/Dockerfile .

docker run ^
       -it ^
       --rm ^
       --mount type=bind,source="%CD%",target=/home/dagger/htcondor-dags ^
       -p 8000:8000 ^
       %CONTAINER_TAG% ^
       bash -c "ptw --poll"
