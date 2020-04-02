*******************************************************************************************
* Autor : Adil Ziani El Ali
* Fecha : 02/04/2020
*******************************************************************************************

En este ejemplo montamos una arquitectura lambda. Hacemos uso de HDFS, Kafka, SparkStreaming
y la API de Twitter (se necesitan credenciales para completar /src/main/resources/twitter.conf).

El ejemplo se organiza en 3 ejercicios:

Ejercicio0: iniciar StreamingContext que consume de Kafka los tweets que van llegando y 
mostrarlos por pantalla

Ejercicio1: implementa la fase de Speed layer, inicia StreamingContext que consume de Kafka los tweets
y los almacena en HDFS a modo DataLake sin ninguna gestion, y guarda cierta información como usuario, 
localización y hashtags en MongoDB

Ejercicio2: implementa la fase Batch layer, consume los tweets en hdfs para hacer un análisis sentimental 
de éstos y guardar el resultado en mongoDB.

Nota: en el fichero Kafka_topic exite información sobre pasos a seguir antes de ejecutar el programa

