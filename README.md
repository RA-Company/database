# Simple Database interface with logging

Connect and execute to database with logging:
- PostgreSQL
- Redis
- ClickHouse

# Used libraries
* https://github.com/jackc/pgx/ - PostgreSQL Driver and Toolkit  (MIT License)
* https://github.com/georgysavva/scany/ - Scans data into Go objects from the database (MIT license)
* https://github.com/redis/go-redis/ - Redis client for Go (BSD-2-Clause license)
* https://github.com/ClickHouse/clickhouse-go/ - Golang SQL database client for ClickHouse (Apache-2.0 license)

# Staying up to date
To update library to the latest version, use go get -u github.com/ra-company/database.

# Supported go versions
We currently support the most recent major Go versions from 1.26.4 onward.

# SAST
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=RA-Company_database&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=RA-Company_database)

# License
This project is licensed under the terms of the GPL-3.0 license.