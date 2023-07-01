FROM 755674844232.dkr.ecr.us-east-1.amazonaws.com/spark/emr-6.9.0:latest
USER root
RUN amazon-linux-extras install python3.8; rm /usr/bin/python /usr/bin/python3; ls /usr/bin/python* /usr/bin/pip* -la ; ln -s /usr/bin/python3.8 /usr/bin/python ; ln -s /usr/bin/python3.8 /usr/bin/python3; sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
USER hadoop:hadoop
RUN python -V; pip3.8 install --upgrade pip; pip3.8 install --upgrade \
    filelock==3.6.0 \
    matplotlib==3.5.2 \
    numpy==1.24.2 \
    pandas==1.4.3 \
    pyarrow==8.0.0 \
    pyspark-stubs~=3.0.0 \
    pyspark~=3.0.0 \
    scikit-learn==1.0.2 \
    scikit-survival==0.17.2 \
    scipy==1.8.1 \
    seaborn==0.11.2 \
    joblib~=1.1.0 \
    binpacking==1.5.2 \
    lifelines==0.27.4 \ 
    typing_extensions==4.4.0
RUN echo "export PATH=\"/home/hadoop/.local/bin:\$PATH\"" >> ~/.bashrc && source ~/.bashrc ; printenv ; ls ~ -a

