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
└───workspace_insomnia
│   │   Captura-lookuptables-Insomnia.json
│
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

Si no dispone de su applicationId, vea la sección "Consultas a la API REST" en este documento.
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

>**Observación:** El procedimiento anterior genera automáticamente el archivo `conf/custom_des.xml` con la configuración de los extractions units, si por algún motivo desea configurar este archivo manualmente, luego de ejecutar `scripts/prepare-config-templates.sh` se genera el archivo `conf/custom_des.sample.xml`, el cual puede utilizar como ejemplo para configurar manualmente su `conf/custom_des.xml`.
## Consultas a la API REST
En el directorio `workspace_insomnia/` se encuentra `Captura-lookuptables-Insomnia.json`, un workspace de Insomnia para utilizar la API REST de Captura. Al importarlo dentro de la aplicación de Insomnia dispondrá de las siguientes requests:
* **login**: Iniciar sesión, recibe la lista de aplicaciones disponibles con sus "applicationId" y "label" correspondientes; esta request obtiene una cookie necesaria para utilizar las demás requests.
* **crear lookupTables**: Crea un lookUpTable según la definición dada.
* **listar lookUpTables**: Recibe la definición de un LookUpTable, un LookUpTable viene identificado por el campo "pk", el cual se coloca en la url de esta request.
* **listar todas las lookUpTables**: lista todas las lookUpTables del usuario, retorna "identifier", "acceptRESTDMLs", "pk", "applicationId" y "name" de las lookUpTables.
* **insertLookupTableValue**: Insertar registros a un lookUpTable, la "pk" del lookUpTable correspondiente se coloca en la url de esta request.
* **list lookupTableData**: lista los registros de un lookUpTable, la "pk" del lookUpTable se coloca en la url de esta request.
* **listar aplicaciones**: lista los proyectos del usuario.



