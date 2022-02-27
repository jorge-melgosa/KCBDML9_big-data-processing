# Modulo: Big Data Processing
`Fecha máxima de entrega: 27 de Febrero 2022`

## Practica
### Enunciado:
En este proyecto vamos a construir una [arquitectura lambda](https://en.wikipedia.org/wiki/Lambda_architecture) para el procesamiento de datos recolectados desde antenas de telefonía móvil. Una arquitectura lambda se separa en tres capas:

* **Speed Layer**: Capa de procesamiento en streaming. Computa resultados en tiempo real y baja latencia.
* **Batch Layer**: Capa de procesamiento por lotes. Computa resultados usando grande cantidades de datos, alta latencia.
* **Serving Layer**: Capa encargada de servir los datos, es nutrida por las dos capas anteriores.

En nuestro proyecto usaremos las distintas tecnologías para cubrir los requisitos demandados por las distintas capas:

* **Speed Layer**: [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html), [Apache Kafka](https://kafka.apache.org/), [Google Compute Engine](https://cloud.google.com/compute)
* **Batch Layer**: [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html), [Google Cloud Storage](https://cloud.google.com/storage)
* **Serving Layer**: [Google SQL(PostgreSQL)](https://cloud.google.com/sql/docs/postgres), [Apache Superset](https://superset.incubator.apache.org/)



### Resumen de la implementación: ###

Tenemos las siguientes partes diferenciadas dentro del proyecto:

* **Generación de tablas necesarias**: Tendrá las sentencias necesarias para la preparación de la bbdd que vamos a utilizar tanto para el guardado de los datos procesados como para el enriquecimiento de los datos.
* **Linea de procesamiento en streaming**: Tendrá el desarrollo necesario para el procesamiento de en streaming. Lectura de datos, procesamiento de los mismos y almacenamiento en bbdd y en disco.
* **Linea de procesamiento por lotes**: Tendrá el desarrollo necesario para el procesamiento de los datos por lotes. Lectura de los datos en disco anteriormente almacenados, procesamiento y almacenamiento en bbdd.
* **Visual 


---
---


### Arquitectura.
Para la realización necesitamos disponer de diferentes sistemas que estarán distribuidos en la nube (Google Cloud) y en local:

* **Maquina virtual**: Dentro del servicio Compute Engine vamos a crear una maquina virtual donde tendremos:
	* **Kafka**: Instalaremos y configuraremos un broker de Kafka donde se recibirán los mensajes en tiempo real.	
	* **Docker**: Con una imagen proporcionada por el profesor que realizará las funciones de envío de datos a nuestro sistema, simulando lo que podrían ser elementos IoT en la realidad.
	* **Base de datos**: Utilizaremos PostgesSQL para el almacenamiento de los datos procesados y para almacenar la información propia del sistema en la que nos apoyaremos en algunos casos para enriquecer estos procesamientos.

 		Para preparar la base de datos utilizaremos un job de aprovisionamiento implementado dentro del proyecto en el archivo *provisioner/JdbcProvisioner.scala* donde está implementada la creación de las diferentes tablas y la inicialización de la tabla maestra de metadatos.

* **Sistema de Ficheros**: Para el almacenamiento disco utilizaremos el entorno local donde se ejecuta el proyecto, que en mi caso es en local a través del ide. Por lo que tendremos en local la ruta */tmp/project* donde almacenaremos en formato parquet la información original recibida.



### Implementación procesamiento en streaming
En esta parte realizaremos el desarrollo del procesamiento en streaming de los datos que nos llegan de los dispositivos IoT "simulados".

La implementación esta realizada en el archivo *streaming/StreamingJobImpl.scala* que extiende de Tair *streaming/StreamingJob.scala* donde tenemos definidos los métodos a implementar y la definición de los mismos.

Tenemos lo siguiente:

* Creamos un contexto de Spark para poder gestionar todo el procesamiento.
* **readFromKafka**: Realizará la lectura del sistema Kafka a un topic concreto que le indicaremos y generaremos el DataFrame original con el que comenzaremos a trabajar.
* **parserJsonData**: En este método realizamos la conversión a JSON del DataFrame que nos llega desde *readFromKafka* para tener los datos en un formato que nos ayude a la visualización y el trabajo. A partir de aquí en podremos dar respuesta a las necesidades que se nos traslada.
* **computeBytesReceivedPerAntenna**: Total de bytes recibidos por antena cada 5 minutos. En este método nos quedamos con los campos del DataFrame que vamos a necesitar (el momento exacto de la generación, la antena y los bytes recibidos). Después agrupamos por la antena para después poder realizar la agregación donde se realizará la suma de los bytes por antena. Una vez que tenemos los datos los preparamos en el formato de salida que vamos a utilizar en su almacenamiento, será necesario agregar una columna nueva para poder indicarle el tipo de agregación que hemos realizado.
* **computeBytesTransmittedPerUser**: Total de bytes transmitidos por el id de usuario cada 5 minutos. En este método nos quedamos con los campos del DataFrame que vamos a necesitar (El momento exacto de la generación, el id del usuario y los bytes transmitidos). Después agrupamos por el id del usuario para después poder realizar la agregación donde se realizará la suma de los bytes por usuario. Una vez que tenemos los datos los preparamos en el formato de salida que vamos a utilizar en su almacenamiento, será necesario agregar una columna nueva para poder indicarle el tipo de agregación que hemos realizado.
*  **computeBytesTransmittedPerApplication**: Total de bytes transmitidos por aplicación cada 5 minutos. En este método nos quedamos con los campos del DataFrame que vamos a necesitar (El momento exacto de la generación, la aplicación y los bytes transmitidos). Después agrupamos por la aplicación para después poder realizar la agregación donde se realizará la suma de los bytes por usuario. Una vez que tenemos los datos los preparamos en el formato de salida que vamos a utilizar en su almacenamiento, será necesario agregar una columna nueva para poder indicarle el tipo de agregación que hemos realizado.
* **writeToJdbc**: Almacenamiento de la metrica en la base de datos. En este método realizaremos el almacenamiento por lotes de la información que nos está llegando y estamos procesando, mientras tengamos el sistema levantado.
* **writeToStorage**: Almacenamiento de la información recibida de nuestro sistema IoT simulado, sin procesarla para su posterior tratamiento. Lo archivaremos separado por año, mes, dia y hora.
* **main**: Es el proceso que ejecutará todo el job. Donde crearemos los datos de configuración necesarios para la ejecución y llamaremos al método *run* que se encargará de realizar la ejecución.


### Implementación procesamiento en lotes
En esta parte realizaremos el desarrollo del procesamiento por lotes, donde nuestra fuente de datos será los ficheros de tipo parquet que tenemos almacenados con la información recogida del procesamiento por streaming.

La implementación esta realizada en el archivo *batch/BatchJobImpl.scala* que extiende de Tair *batch/BatchJob.scala* donde tenemos definidos los métodos a implementar y la definición de los mismos.

Tenemos lo siguiente:

* Creamos un contexto de Spark para poder gestionar todo el procesamiento.
* **readFromStorage**: Realizaremos la lectura de los datos almacenados en nuestro sistema de ficheros. Para ello nos conectaremos en nuestro caso a la maquina local y localizaremos los archivos que tenemos particionados por año, mes, día y hora. Esto nos dará el primer DataFrame con el comenzaremos a trabajar.
* **readUserMetadata**: En este método realizamos la lectura de los datos que tenemos en la base de datos y que nos permitirá enriquecer la información que nos llega desde los dispositivos IoT simulados. Para ello nos conectaremos a la base de datos y la tabla correspondiente.
* **enrichDeviceWithUserMetadata**: En este método uniremos por el id del usaurio la informacióm que nos ha llegado desde los dispositivos y tenemos almacenada en nuestro sistema de archivos y la información que tenemos en la base de datos. Con esto obtenemos un segundo DataFrame con mucha mas información donde hemos aprovechado para eliminar la información duplicada.
* **computeBytesReceivedPerAntenna**: Total de bytes recibidos por antena cada hora. En este método nos quedamos con los campos del DataFrame que vamos a necesitar (el momento exacto de la generación, la antena y los bytes recibidos). Después agrupamos por la antena para despues poder realizar la agregación donde se realizará la suma de los bytes por antena. Una vez que tenemos los datos los preparamos en el formato de salida que vamos a utilizar en su almacenamiento, será necesario agregar una columna nueva para poder indicarle el tipo de agregación que hemos realizado.
*  **computeBytesTransmittedPerUserMail**: Total de bytes transmitidos por el mail de usuario cada hora. En este método nos quedamos con los campos del DataFrame que vamos a necesitar (El momento exacto de la generación, el mail del usuario y los bytes transmitidos). Después agrupamos por el mail del usuario para después poder realizar la agregación donde se realizará la suma de los bytes por usuario. Una vez que tenemos los datos los preparamos en el formato de salida que vamos a utilizar en su almacenamiento, será necesario agregar una columna nueva para poder indicarle el tipo de agregación que hemos realizado.
*  **computeBytesTransmittedPerApplication**: Total de bytes transmitidos por aplicación cada hora. En este método nos quedamos con los campos del DataFrame que vamos a necesitar (El momento exacto de la generación, la aplicación y los bytes transmitidos). Después agrupamos por la aplicación para después poder realizar la agregación donde se realizará la suma de los bytes por usuario. Una vez que tenemos los datos los preparamos en el formato de salida que vamos a utilizar en su almacenamiento, será necesario agregar una columna nueva para poder indicarle el tipo de agregación que hemos realizado.
* **computeUserWhoHaveExceededTheHourlyQuota**: Obtendremos las cuantas de usuario que han sobrepasado la cuota indicada por hora. Para ello lo primero que haremos será quedarnos con los datos necesarios (El momento exacto de la generación, el mail del usuario, los bytes transmitidos y la cuota). Después agrupamos la información por usuario para poder hacer la suma de bytes que han utilizado los usuarios en una hora. Una vez que tenemos estos datos nos quedamos solo con aquellos que han sobrepasado la cuota. Una vez que tenemos los datos los preparamos en el formato de salida que vamos a utilizar en su almacenamiento, será necesario agregar una columna nueva para poder indicarle el tipo de agregación que hemos realizado.
* **writeToJdbc**: Almacenamiento de la métrica en la base de datos. En este método realizaremos el almacenamiento por lotes de la información que nos está llegando y estamos procesando, mientras tengamos el sistema levantado.
* **main**: Es el proceso que ejecutará todo el job. Donde crearemos los datos de configuración necesarios para la ejecución y llamaremos al metodo *run* que se encargará de realizar la ejecución.


### Visualización de resultado

**Thumbmails de PostgresSQL**

*Tabla bytes*

![Captura de pantalla 2022-02-27 a las 13 15 47](https://user-images.githubusercontent.com/2152086/155883042-27fe845b-b838-4424-a0c9-91fd8b2c25fd.png)


*Tabla bytes_hourly*

![Captura de pantalla 2022-02-27 a las 13 18 18](https://user-images.githubusercontent.com/2152086/155883051-9f54e3a4-0efd-4eb3-af2c-8d95e6872af7.png)

![Captura de pantalla 2022-02-27 a las 13 18 52](https://user-images.githubusercontent.com/2152086/155883054-cbd551bc-fc1e-4cb3-8572-b5f90d5eb722.png)


*Tabla user_quota_limit*

![Captura de pantalla 2022-02-27 a las 13 21 00](https://user-images.githubusercontent.com/2152086/155883061-1988e3e4-0905-49ee-977b-44ff7d767a5f.png)


**Thumbmails de Sistema de archivos**
![Captura de pantalla 2022-02-27 a las 14 16 48](https://user-images.githubusercontent.com/2152086/155884022-cd8f2e2d-7dc0-4f51-b56e-c560eadb4124.png)


**Thumbmails de Superset**

![Captura de pantalla 2022-02-27 a las 13 47 13](https://user-images.githubusercontent.com/2152086/155883072-60bebd8c-2605-41a5-a2b2-e19c770f246b.png)

![Captura de pantalla 2022-02-27 a las 13 48 06](https://user-images.githubusercontent.com/2152086/155883074-c9767089-87c9-4857-8377-556631dc19ff.png)

![Captura de pantalla 2022-02-27 a las 13 48 43](https://user-images.githubusercontent.com/2152086/155883083-e7c00cdd-b596-48cf-b816-4c6cf99fb4b3.png)

