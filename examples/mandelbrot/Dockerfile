FROM htcondor/htc-base-notebook:latest

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends imagemagick-6.q16 \
 && apt-get -y clean \
 && rm -rf /var/lib/apt/lists/*

USER jovyan

RUN conda install graphviz \
 && pip install graphviz

COPY --chown=jovyan:0 . /home/jovyan/htcondor-dags
RUN pip install -e /home/jovyan/htcondor-dags \
 && pip install --upgrade htcondor==8.9.4b1

WORKDIR /home/jovyan/htcondor-dags/examples/mandelbrot
