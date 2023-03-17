import psycopg2
from psycopg2 import OperationalError

# Пароль пользователя
password = "123456"
# Название базы данных
name_database = "information_about_job_vacancies_for_people_with_disabilities"
# Адрес директории проекта
dir_addres = """D:/python/Information_about_job_vacancies_for_people_with_disabilities/source"""


# Функция подключения к базе данных
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

# Подключение к базе данных
connection = create_connection(
    "postgres", "postgres", password, "127.0.0.1", "5432"
)

# Функция создания базы данных
def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

# Создание базы данных
create_database_query = "CREATE DATABASE " + name_database
create_database(connection, create_database_query)

# Подключение к созданной базе данных
connection = create_connection(
    name_database, "postgres", password, "127.0.0.1", "5432"
)

# Функция для организации таблиц
def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


# Создание вспомогательной таблицы
create_workers_table = """
CREATE TABLE IF NOT EXISTS workers (
    prof VARCHAR(1000),
    number VARCHAR(50),
    global_id BIGINT NOT NULL,
    date DATE,
    count_vacancy BIGINT,
    special_work_place VARCHAR(1000),
    specification VARCHAR(1000),
    min_zarplat DECIMAL(9, 2),
    max_zarplat DECIMAL(9, 2),
    work_function VARCHAR(1000),
    work_regim VARCHAR(1000),
    work_osob VARCHAR(1000),
    dop_workers_parameters VARCHAR(1000),
    work_type VARCHAR(1000),
    education VARCHAR(1000),
    prof_stage DECIMAL(9, 2),
    work_place_adm_area VARCHAR(1000),
    work_place_adm_district VARCHAR(1000),
    work_place_adm_location VARCHAR(1000),
    full_name VARCHAR(1000),
    contact_name VARCHAR(1000),
    phone VARCHAR(1000),
    email VARCHAR(1000)
)
"""

execute_query(connection, create_workers_table)


# Заполнение вспомогательной таблицы
fill_workers_table = """
COPY workers from '""" + dir_addres + """\output.csv' DELIMITER ',' CSV HEADER
"""

execute_query(connection, fill_workers_table)


# Создание и заполнение основных отношений
create_fill_prof_table = """
CREATE TABLE prof(
    prof_id BIGSERIAL NOT NULL PRIMARY KEY,
	prof VARCHAR(1000)
);
INSERT INTO prof (prof)
(
    SELECT DISTINCT prof 
    FROM workers
);
"""

execute_query(connection, create_fill_prof_table)


create_fill_special_work_place_table = """
CREATE TABLE special_work_place(
    special_work_place_id BIGSERIAL NOT NULL PRIMARY KEY,
	special_work_place VARCHAR(1000)
);
INSERT INTO special_work_place (special_work_place)
(
    SELECT DISTINCT special_work_place 
    FROM workers
);
"""

execute_query(connection, create_fill_special_work_place_table)


create_fill_work_regim_table = """
CREATE TABLE work_regim(
    work_regim_id BIGSERIAL NOT NULL PRIMARY KEY,
	work_regim VARCHAR(1000)
);
INSERT INTO work_regim (work_regim)
(
    SELECT DISTINCT work_regim 
    FROM workers
);
"""

execute_query(connection, create_fill_work_regim_table)


create_fill_work_osob_table = """
CREATE TABLE work_osob(
    work_osob_id BIGSERIAL NOT NULL PRIMARY KEY,
	work_osob VARCHAR(1000)
);
INSERT INTO work_osob (work_osob)
(
    SELECT DISTINCT work_osob 
    FROM workers
);
"""

execute_query(connection, create_fill_work_osob_table)


create_fill_work_type_table = """
CREATE TABLE work_type(
    work_type_id BIGSERIAL NOT NULL PRIMARY KEY,
	work_type VARCHAR(1000)
);
INSERT INTO work_type (work_type)
(
    SELECT DISTINCT work_type 
    FROM workers
);
"""

execute_query(connection, create_fill_work_type_table)


create_fill_education_table = """
CREATE TABLE education(
    education_id BIGSERIAL NOT NULL PRIMARY KEY,
	education VARCHAR(1000)
);
INSERT INTO education (education)
(
    SELECT DISTINCT education 
    FROM workers
);
"""

execute_query(connection, create_fill_education_table)


create_fill_work_place_adm_area_table = """
CREATE TABLE work_place_adm_area(
    work_place_adm_area_id BIGSERIAL NOT NULL PRIMARY KEY,
	work_place_adm_area VARCHAR(1000)
);
INSERT INTO work_place_adm_area (work_place_adm_area)
(
    SELECT DISTINCT work_place_adm_area 
    FROM workers
);
"""

execute_query(connection, create_fill_work_place_adm_area_table)


# Сведения о вакантных рабочих местах для инвалидов и лиц с ограниченными возможностями, предоставленные
# работодателями в Службу занятости населения города Москвы
create_employee_table = """
CREATE TABLE IF NOT EXISTS employee (
    prof_id  BIGINT REFERENCES prof(prof_id),
    prof VARCHAR(1000),
    number VARCHAR(50),
    global_id BIGINT NOT NULL PRIMARY KEY,
    date DATE,
    count_vacancy BIGINT,
    special_work_place_id BIGINT REFERENCES special_work_place(special_work_place_id),
    special_work_place VARCHAR(1000),
    specification VARCHAR(1000),
    min_zarplat DECIMAL(9, 2),
    max_zarplat DECIMAL(9, 2),
    work_function VARCHAR(1000),
    work_regim_id BIGINT REFERENCES work_regim(work_regim_id),
    work_regim VARCHAR(1000),
    work_osob_id BIGINT REFERENCES work_osob(work_osob_id),
    work_osob VARCHAR(1000),
    dop_workers_parameters VARCHAR(1000),
    work_type_id BIGINT REFERENCES work_type(work_type_id),
    work_type VARCHAR(1000),
    education_id BIGINT REFERENCES education(education_id),
    education VARCHAR(1000),
    prof_stage DECIMAL(9, 2),
    work_place_adm_area_id BIGINT REFERENCES work_place_adm_area(work_place_adm_area_id),
    work_place_adm_area VARCHAR(1000),
    work_place_adm_district VARCHAR(1000),
    work_place_adm_location VARCHAR(1000),
    full_name VARCHAR(1000),
    contact_name VARCHAR(1000),
    phone VARCHAR(1000),
    email VARCHAR(1000)
)
"""

execute_query(connection, create_employee_table)


# Заполнение таблицы данными о вакантных рабочих местах для инвалидов и лиц с ограниченными возможностями,
# в том числе и из связанных таблиц
fill_employee_table = """
INSERT INTO employee (
    prof, number, global_id, date, count_vacancy, special_work_place,
    specification, min_zarplat, max_zarplat, work_function, work_regim,
    work_osob, dop_workers_parameters, work_type, education, prof_stage,
    work_place_adm_area, work_place_adm_district, work_place_adm_location,
    full_name, contact_name, phone, email
)
(SELECT 
    prof, number, global_id, date, count_vacancy, special_work_place,
    specification, min_zarplat, max_zarplat, work_function, work_regim,
    work_osob, dop_workers_parameters, work_type, education, prof_stage,
    work_place_adm_area, work_place_adm_district, work_place_adm_location,
    full_name, contact_name, phone, email 
FROM workers)
"""

execute_query(connection, fill_employee_table)


update_employee_table_prof = """
UPDATE employee AS e
SET prof_id = (
    SELECT prof.prof_id 
    FROM prof 
    WHERE prof.prof = e.prof
)
"""

execute_query(connection, update_employee_table_prof)


update_employee_table_education = """
UPDATE employee AS e
SET education_id = (
    SELECT education.education_id 
    FROM education 
    WHERE education.education = e.education
)
"""

execute_query(connection, update_employee_table_education)


update_employee_table_special_work_place = """
UPDATE employee AS e
SET special_work_place_id = (
    SELECT special_work_place.special_work_place_id
    FROM special_work_place 
    WHERE special_work_place.special_work_place = e.special_work_place
)
"""

execute_query(connection, update_employee_table_special_work_place)


update_employee_table_work_osob = """
UPDATE employee AS e
SET work_osob_id = (
    SELECT work_osob.work_osob_id
	FROM work_osob 
	WHERE work_osob.work_osob = e.work_osob
)
"""

execute_query(connection, update_employee_table_work_osob)


update_employee_table_work_place_adm_area = """
UPDATE employee AS e
SET work_place_adm_area_id = (
    SELECT work_place_adm_area.work_place_adm_area_id
	FROM work_place_adm_area 
	WHERE work_place_adm_area.work_place_adm_area = e.work_place_adm_area
)
"""

execute_query(connection, update_employee_table_work_place_adm_area)


update_employee_table_work_regim = """
UPDATE employee AS e
SET work_regim_id = (
    SELECT work_regim.work_regim_id
	FROM work_regim 
	WHERE work_regim.work_regim = e.work_regim
)
"""

execute_query(connection, update_employee_table_work_regim)


update_employee_table_work_type = """
UPDATE employee AS e
SET work_type_id = (
    SELECT work_type.work_type_id
	FROM work_type 
	WHERE work_type.work_type = e.work_type
)
"""

execute_query(connection, update_employee_table_work_type)


# Удаление излишней информации
alter_employee_table = """
ALTER TABLE employee 
    DROP COLUMN prof, 
    DROP COLUMN education, 
    DROP COLUMN special_work_place, 
    DROP COLUMN work_osob, 
    DROP COLUMN work_place_adm_area,
    DROP COLUMN work_regim, 
    DROP COLUMN work_type
"""

execute_query(connection, alter_employee_table)


# Удаление вспомогательной таблицы
drop_workers_table = """
DROP TABLE workers
"""

execute_query(connection, drop_workers_table)