// iniciar cassandra ----------------------------

sudo update-alternatives --config java // cambiar versión de java

sudo service cassandra start // or status

cqlsh // entrar a la consola

// ----------------------------------------------

// Crear keyspace
CREATE KEYSPACE IF NOT EXISTS empleados WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };

// entrar a la db
use empleados;

// crear tabla Q1
CREATE TABLE empleados_por_habilidad_y_deporte (
	id_empleado UUID,
  nombre_empleado text,
	apellido_empleado text,
	descripcion_habilidad text,
	nombre_deporte text,
	PRIMARY KEY ((descripcion_habilidad, nombre_deporte), id_empleado)
);

// crear tabla Q2
CREATE TABLE trabajos_previos_por_habilidad (
	id_empleado UUID,
	nombre_empleado text,
	apellido_empleado text,
	descripcion_habilidad text,
	funcion_trabajo text,
	PRIMARY KEY ((descripcion_habilidad), id_empleado, funcion_trabajo text)
);

// crear tabla Q3
CREATE TABLE deportes_por_trabajos_previos_entre_fechas (
	id_empleado UUID,
	nombre_empleado text,
	apellido_empleado text,
	nombre_deporte text,
	inicio_trabajo timestamp,
	fin_trabajo timestamp,
	PRIMARY KEY ((inicio_trabajo, fin_trabajo), id_empleado)
);

// crear tabla Q4
CREATE TABLE empleados_por_deporte_de_riesgo_desde_fecha (
	id_empleado UUID,
	nombre_empleado text,
	apellido_empleado text,
	nombre_deporte text,
	riesgo_deporte text,
	inicio_deporte timestamp,
	PRIMARY KEY ((riesgo_deporte, inicio_deporte), id_empleado)
);

// crear tabla Q5
CREATE TABLE habilidades_por_empleado (
	id_empleado UUID,
	nombre_empleado text,
	apellido_empleado text,
	descripcion_habilidad text,
	PRIMARY KEY ((id_empleado), descripcion_habilidad)
);

// crear tabla Q6
CREATE TABLE deportes_por_trabajo (
	id_empleado UUID,
	nombre_empleado text,
	apellido_empleado text,
	funcion_trabajo text,
	nombre_deporte text,
	PRIMARY KEY ((funcion_trabajo), id_empleado)
);

// crear tabla Q7
CREATE TABLE empleados_por_trabajo_deporte_federado (
	id_empleado UUID,
	nombre_empleado text,
	apellido_empleado text,
	funcion_trabajo text,
	federado_deporte boolean,
	PRIMARY KEY ((funcion_trabajo, federado_deporte), id_empleado)
);

// crear tabla Q8
CREATE TABLE empleados_por_habilidad_y_trabajo_previo_entre_fechas (
	id_empleado UUID,
	nombre_empleado text,
	apellido_empleado text,
	descripcion_habilidad text,
	funcion_trabajo text,
	inicio_trabajo timestamp,
	fin_trabajo timestamp,
	PRIMARY KEY ((descripcion_habilidad, funcion_trabajo, inicio_trabajo, fin_trabajo), id_empleado)
);



// insertar datos Q1
INSERT INTO empleados_por_habilidad_y_deporte (id_empleado, nombre_empleado, apellido_empleado, descripcion_habilidad, nombre_deporte) VALUES (uuid(), 'Carlos', 'Rodriguez', 'Liderazgo', 'Futbol');

// insertar datos Q2
INSERT INTO trabajos_previos_por_habilidad (id_empleado, nombre_empleado, apellido_empleado, descripcion_habilidad, funcion_trabajo) VALUES (uuid(), 'Maximo', 'Puyol', 'Especialista en Procesos', 'Contador');

// insertar datos Q3
INSERT INTO deportes_por_trabajos_previos_entre_fechas (id_empleado, nombre_empleado, apellido_empleado, nombre_deporte, inicio_trabajo, fin_trabajo) VALUES (uuid(), 'Pedro', 'Linch', 'Tenis', '2011-02-03', '2017-08-10');

// insertar datos Q4
INSERT INTO empleados_por_deporte_de_riesgo_desde_fecha (id_empleado, nombre_empleado, apellido_empleado, nombre_deporte, riesgo_deporte, inicio_deporte) VALUES (uuid(), 'Ignacio', 'Prados', 'Snowboard', 'Alto', '2022-01-01');

// insertar datos Q5
INSERT INTO habilidades_por_empleado (id_empleado, nombre_empleado, apellido_empleado, descripcion_habilidad) VALUES (uuid(), 'Facundo', 'Carlanzo', 'Comunicacion');

// insertar datos Q6
INSERT INTO deportes_por_trabajo (id_empleado, nombre_empleado, apellido_empleado, funcion_trabajo, nombre_deporte) VALUES (uuid(), 'Sergio', 'Sirio', 'Programador', 'Paddle');

// insertar datos Q7
INSERT INTO empleados_por_trabajo_deporte_federado (id_empleado, nombre_empleado, apellido_empleado, funcion_trabajo, federado_deporte) VALUES (uuid(), 'Mariana', 'Mises', 'Contador', true);

// insertar datos Q8
INSERT INTO empleados_por_habilidad_y_trabajo_previo_entre_fechas (id_empleado, nombre_empleado, apellido_empleado, descripcion_habilidad, funcion_trabajo, inicio_trabajo, fin_trabajo) VALUES (uuid(), 'Martina', 'Quiston', 'Experto Programador', 'Comunicacion','2011-05-06', '2019-07-11');



// Ejecutar Q1
SELECT * FROM empleados_por_habilidad_y_deporte WHERE descripcion_habilidad='Liderazgo' AND nombre_deporte='Futbol';

// Ejecutar Q2
SELECT * FROM trabajos_previos_por_habilidad WHERE descripcion_habilidad='Especialista en Procesos';

// Ejecutar Q3
SELECT * FROM deportes_por_trabajos_previos_entre_fechas WHERE inicio_trabajo>='2010-01-01' AND fin_trabajo<='2020-01-01' ALLOW FILTERING;

// Ejecutar Q4
SELECT * FROM empleados_por_deporte_de_riesgo_desde_fecha WHERE riesgo_deporte='Alto' AND inicio_deporte>='2021-01-01' ALLOW FILTERING;

// Ejecutar Q5 -- Cambiar UUID
SELECT * FROM habilidades_por_empleado WHERE id_empleado=2f46338c-a4c9-49da-88fe-9c944dcd3fd2;

// Ejecutar Q6
SELECT * FROM deportes_por_trabajo WHERE funcion_trabajo='Programador';

// Ejecutar Q7
SELECT * FROM empleados_por_trabajo_deporte_federado WHERE funcion_trabajo='Contador' AND federado_deporte=true;

// Ejecutar Q8
SELECT * FROM empleados_por_habilidad_y_trabajo_previo_entre_fechas WHERE descripcion_habilidad='Experto Programador' AND funcion_trabajo='Comunicacion' AND inicio_trabajo>='2010-01-01' AND fin_trabajo<='2020-01-01' ALLOW FILTERING;
