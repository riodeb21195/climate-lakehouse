-- One Postgres instance serves both metadata stores; each gets its own database.
CREATE DATABASE lakekeeper;
CREATE DATABASE airflow;
