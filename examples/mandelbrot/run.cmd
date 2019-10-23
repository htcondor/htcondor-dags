@echo off

SET CONTAINER_TAG=htcondor-dags-mandelbrot-example

docker build -t %CONTAINER_TAG% --file examples/mandelbrot/Dockerfile . || exit /b
docker run -it --rm -p 8888:8888 %CONTAINER_TAG% %* || exit /b
