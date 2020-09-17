from confluent_kafka import Consumer, Producer
from redis import Redis as getRedis
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from time import sleep
from sys import stdout
from os import getenv

class Kafka:
    """
    Clase que engloba los metodos encargados de conectarse a Kafka. Ofrece dos metodos
    estaticos:
    - getConsumer()
    - getProducer()
    """
    __consumer = None
    __producer = None
    @staticmethod
    def getConsumer():
        """
        Metodo estatico que sigue el patron de diseno Singleton. Para su funcionamiento
        require las siguientes variables de entorno:
        - KAFKA_HOST: direccion en la cual se encuentra Kafka
        - KAFKA_GROUPID: grupo al cual la aplicacion pertenece
        - KAFKA_TOPIC: topico al cual la aplicacion debe subscribirse
        Se asume que Kafka esta corriendo en el puerto por defecto (9092)

        Retorna instancia de confluent_kafka.Consumer
        """
        if not Kafka.__consumer:
            host = getenv('KAFKA_HOST')
            group = getenv('KAFKA_GROUPID')
            topic = getenv('KAFKA_TOPIC')
            Kafka.__consumer = Consumer({
                'bootstrap.servers': host,
                'group.id': group,
                'auto.offset.reset': 'beginning'
            })
            Kafka.__consumer.subscribe([kafka_topic])
        return Kafka.__consumer
    @staticmethod
    def getProducer():
        """
        Metodo estatico que sigue el patron de diseno Singleton. Para su funcionamiento
        require las siguientes variables de entorno:
        - KAFKA_HOST: direccion en la cual se encuentra Kafka
        Se asume que Kafka esta corriendo en el puerto por defecto (9092)

        Retorna instancia de confluent_kafka.Producer
        """
        if not Kafka.__producer:
            host = getenv('KAFKA_HOST')
            Kafka.__producer = Producer({
                'bootstrap.servers': host
            })
        return Kafka.__producer

class Cassandra:
    """
    Clase que engloba los metodos encargados de conectarse a Cassandra. Ofrece 3 metodos
    estaticos: getInstance(), addQuery(string), query(string)
    """
    __instance = None
    __statement = None
    @staticmethod
    def getInstance():
        """
        Metodo estatico que sigue el patron de diseno Singleton. Para su funcionamiento
        require las siguientes variables de entorno:
        - CASSANDRA_USER: usuario de Cassandra
        - CASSANDRA_PASSWORD: contrasena de Cassandra
        - CASSANDRA_HOST: direccion de Cassandra
        - CASSANDRA_PORT: puerto de Cassandra
        - CASSANDRA_KEYSPACE: keyspace de Cassandra
        Si es que el server no se encuentra disponible, intenta reconectarse 6 veces con
        un intervalo de 10 segundos entre si

        Retorna una instancia de Cassandra conectada al keyspace indicado
        """
        if not Cassandra.__instance:
            host = getenv('CASSANDRA_HOST')
            port = getenv('CASSANDRA_PORT')
            user = getenv('CASSANDRA_USER')
            password = getenv('CASSANDRA_PASSWORD')
            keyspace = getenv('CASSANDRA_KEYSPACE')
            auth_provider = PlainTextAuthProvider(username=user, password=password)
            cluster = Cluster([host], port=port, auth_provider=auth_provider)
            Cassandra.__instance = cluster.connect(keyspace)
        return Cassandra.__instance
    @staticmethod
    def addQuery(statement):
        """
        Metodo estatico que agrega una expresion a Cassandra para realizar querys.
        Ejemplo: 'SELECT info FROM deteccion WHERE objectid=?'
        """
        Cassandra.__statement = Cassandra.__instance.prepare(statement)
    @staticmethod
    def query(query):
        """
        Metodo estatico que ejecuta la expresion agregada en addQuery()
        """
        return Cassandra.__instance.execute(Cassandra.__statement, [query])

class Redis:
    """
    Clase que engloba los metodos encargados de conectarse a Redis. Ofrece un metodo
    estatico: getInstance()
    """
    __instance = None
    @staticmethod
    def getInstance():
        """
        Metodo estatico que sigue el patron de diseno Singleton. Para su funcionamiento
        requiere las siguientes variables de entorno:
        - REDIS_HOST: host de Redis
        - REDIS_PORT: port de Redis
        - REDIS_DB: nombre de la base de datos de redis

        Retorna una instancia de Redis conectada a la base de datos especificada
        """
        if not Redis.__instance:
            host = getenv('REDIS_HOST')
            port = getenv('REDIS_PORT')
            db = getenv('REDIS_DB')
            password = getenv('REDIS_PASSWORD')
            Redis.__instance = getRedis(host=host, port=port, db=db, password=password)
        return Redis.__instance

if __name__ == "__main__":
    pass
