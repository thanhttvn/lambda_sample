FROM public.ecr.aws/lambda/python:3.8

WORKDIR /opt/oracle
RUN yum -y install wget \
    && yum -y install unzip \
    && yum -y install libaio \
    && wget https://download.oracle.com/otn_software/linux/instantclient/1915000/instantclient-basic-linux.x64-19.15.0.0.0dbru-2.zip \
    && unzip instantclient-basic-linux.x64-19.15.0.0.0dbru-2.zip \
    && rm -f instantclient-basic-linux.x64-19.15.0.0.0dbru-2.zip \
    && echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && /sbin/ldconfig \
    && export LD_LIBRARY_PATH=/opt/oracle/instantclient_19_5:$LD_LIBRARY_PATH

WORKDIR /
COPY requirements.txt .
RUN yum -y install gcc-c++ python3-devel unixODBC-devel
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
COPY lambda.py ${LAMBDA_TASK_ROOT}

CMD [ "lambda.handler" ]