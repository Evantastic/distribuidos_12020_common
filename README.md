# Libreria comun Sistemas distribuidos

Libreria que tiene conecciones estandar a Kafka, Redis y Cassandra. uso exclusivo para laboratorio sistemas distribuidos 1-2020.

## Como Instalar

Para instalar directamente en el PC
```
git clone https://github.com/Evantastic/distribuidos_common.git
pip install -r distribuidos_common/requirements.txt
pip install ./distribuidos_common
```
Se adjunta un dockerfile para denotar como seria la instalacion en un Dockerfile

## Como usar

Para usar
```python
from distribuidos_common import Kafka, Redis, Cassandra
r = Redis.getInstance()
ck = Kafka.getConsumer()
pk = Kafka.getProducer()
c = Cassandra.getInstance()
```
EL codigo posee documentacion, asi que para entener mejor las clases referirse a la documentacion