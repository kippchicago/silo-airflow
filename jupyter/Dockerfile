# Choose your desired base image
FROM jupyter/base-notebook:latest

# Create a Python 2.x environment using conda including at least the ipython kernel
# and the kernda utility. Add any additional packages you want available for use
# in a Python 2 notebook to the first line here (e.g., pandas, matplotlib, etc.)


COPY ./requirements.txt /tmp/

RUN conda create --quiet --yes -p $CONDA_DIR/envs/python2 python=2.7 --file /tmp/requirements.txt && \
    conda clean -tipsy

USER root

# Create a global kernelspec in the image and modify it so that it properly activates
# the python2 conda environment.
RUN $CONDA_DIR/envs/python2/bin/python -m ipykernel install && \
    $CONDA_DIR/envs/python2/bin/kernda -o -y /usr/local/share/jupyter/kernels/python2/kernel.json

RUN  apt-get update -yqq \
     && apt-get upgrade -yqq \
     && apt-get install -yqq  build-essential
     #&& apt-get install python-devel libxslt-devel libffi-devel openssl-devel

USER $NB_USER

ENV SLUGIFY_USES_TEXT_UNIDECODE=yes

COPY ./requirements.txt /tmp/
#RUN pip install --upgrade pip

#RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org pip setuptools

RUN pip install --upgrade pip && \
    pip install -r /tmp/requirements.txt && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER
