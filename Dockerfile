FROM python:3.7-buster
RUN git clone https://github.com/Evantastic/distribuidos_common.git
RUN pip install -r distribuidos_common/requirements.txt
RUN pip install ./distribuidos_common