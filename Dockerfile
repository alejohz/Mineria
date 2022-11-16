FROM daskdev/dask:latest-py3.10
RUN conda install -c anaconda scikit-learn==1.0.1 -y
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt