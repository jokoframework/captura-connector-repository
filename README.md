# Captura Connector [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
---
> Aplicación para poblar Lookup Tables a una aplicación de Captura Forms.
> Es una aplicación Web Standlone, que utiliza el API REST de Captura para poblar tablas de datos.
>Permite que listas de datos sean pobladas periódicamente desde una consulta a una base de datos mediante JDBC.
## Instalación y Configuración
* Clone el proyecto y descomprímalo, se obtendrán los siguientes archivos:
```
captura-connector-repository
│   README.md
│   mf_cr.sample.properties   
│   ...
└───scripts
│   │   prepare-config-templates.sh
│   ...
```
* Instale manualmente la dependencia necesaria captura-exchange-0.0.6.jar, para ello puede abrir una ventana de terminal, ubicarse en el directorio `captura-connector-repository` y ejecutar:
```sh
mvn install:install-file \
  -DgroupId=py.com.sodep.captura \
  -DartifactId=captura-exchange \
  -Dpackaging=jar \
  -Dversion=0.0.6 \
  -Dfile=./lib/captura-exchange-0.0.6.jar \
  -DgeneratePom=true
```
* Ejecute `scripts/prepare-config-templates.sh`, esto creará el archivo `conf/mf_cr.properties` donde se configuran las distintas credenciales.
* En `conf/mf_cr.properties` debe establecer los parámetros requeridos, como mínimo, la información de autenticación requerida para llegar al servidor de formularios, que son los siguientes cuatro campos:

> mf_cr.rest.user `PUT_HERE_YOUR_CAPTURA_USER`

> mf_cr.rest.pass `PUT_HERE_YOUT_CAPTURA_PASSWORD`

> mf_cr.rest.baseURL `PUT_HERE_YOUR_CAPTURA_BASE_URL`

> mf_cr.rest.applicationId `PUT_HERE_YOUR_CAPTURA_APPLICATION_ID`

* Ahora puede ejecutar el proyecto, ejecute el siguiente comando en una ventana de terminal (en el directorio `captura-connector-repository`):
```sh
./mvnw spring-boot:run
```
### Generación de extractions units
* Una vez que haya iniciado el proyecto, desde su navegador acceda a `http://localhost:8081/`, el usuario y contraseña están definidos en `conf/mf_cr.properties`, por defecto usuario=SOME_RANDOM_WEB_INTERFACE_USER y password=SOME_RANDOM_WEB_INTERFACE_PASS.
* Configure la conexión a la base de datos, para ello seleccione `Add Connection` y complete los campos. Un ejemplo sería:

| | |
| ------ | ------ |
| ID | connection_to_postgresql |
| URL| jdbc:postgresql://localhost:5432/my_db |
| Driver | org.postgresql.Driver |
| User | postgresql_user |
| Password | postgresql_password |
* Testee la conexión seleccionando  `Test Connection`, luego guarda los cambios seleccionando `Save Connection`.
* Una vez configurada la conexión a la base de datos, puede configurar los extractions units, seleccione `Add Extraction Unit`.
* Complete los campos. Un ejemplo sería:

| | |
| ------ | ------ |
| Connection | connection_to_postgresql |
| ID| cities |
| Description | cities |
| SQL | select * from city; |
| Frequency | 100 |
| Batch Size | 1000 |
* Tras ingresar la consulta sql, se mostrarán los columnas relacionadas a la misma, seleccione manualmente su `Primary Key`, luego seleccione `Active` para activarlo y guarde los cambios seleccionando `Save Extraction Unit`.
* Para que los cambios tengan efecto, deberá reiniciar el proyecto.



