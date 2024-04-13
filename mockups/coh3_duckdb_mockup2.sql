-- duckdb d:\data-engineering-zoomcamp2024-project2\coh3.duckdb
use coh3.matches;
show tables;
LOAD motherduck;
CREATE DATABASE clouddb FROM CURRENT_DATABASE();
