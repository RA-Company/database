# Simple Database interface with logging

Connect and execute to database with logging:
- PostgreSQL
- Redis
- ClickHouse

## Staying up to date

```bash
go get -u github.com/ra-company/database
```

## Supported Go versions

Go 1.26.4 and later.

## SAST

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=RA-Company_database&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=RA-Company_database)

## License

MIT — see [LICENSE](LICENSE).

## Dependencies

**Runtime**

| Library | Purpose | License |
|---------|---------|---------|
| [github.com/ClickHouse/clickhouse-go/v2](https://github.com/ClickHouse/clickhouse-go) | ClickHouse client | Apache-2.0 |
| [github.com/georgysavva/scany/v2](https://github.com/georgysavva/scany) | Scan database rows into Go structs | MIT |
| [github.com/google/uuid](https://github.com/google/uuid) | RFC 4122 UUID generation | BSD-3-Clause |
| [github.com/jackc/pgx/v5](https://github.com/jackc/pgx) | PostgreSQL driver and toolkit | MIT |
| [github.com/ra-company/env](https://github.com/ra-company/env) | Environment variable helpers | MIT |
| [github.com/ra-company/logging](https://github.com/ra-company/logging) | Structured logger with context propagation | MIT |
| [github.com/redis/go-redis/v9](https://github.com/redis/go-redis) | Redis client | BSD-2-Clause |

**Tests only**

| Library | Purpose | License |
|---------|---------|---------|
| [github.com/brianvoe/gofakeit/v7](https://github.com/brianvoe/gofakeit) | Fake data generation | MIT |
| [github.com/stretchr/testify](https://github.com/stretchr/testify) | Test assertions | MIT |
