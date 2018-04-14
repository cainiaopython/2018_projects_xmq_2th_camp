-- 先新建数据库，取名“python”

CREATE TABLE city
(
	city_id INT(20) PRIMARY KEY,
	city_name VARCHAR(100) NOT NULL
);

CREATE TABLE comp
(
	comp_id INT(20) PRIMARY KEY,
	comp_name VARCHAR(100) NOT NULL,
	comp_full_name VARCHAR(200) NOT NULL
);

CREATE TABLE postn
(
	postn_id INT(20) PRIMARY KEY,
	postn_name VARCHAR(200),
	postn_salary_max INT(20),
	postn_salary_min INT(20),
	postn_create_time DATETIME,
	comp_id INT(20),
	FOREIGN KEY (comp_id) REFERENCES comp(comp_id),
	city_id INT(20),
	FOREIGN KEY (city_id) REFERENCES city(city_id)
);

