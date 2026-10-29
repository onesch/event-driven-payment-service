```sql
psql -U postgres

CREATE DATABASE <db_name>;

\c <db_name>;

GRANT ALL PRIVILEGES ON SCHEMA public TO <user_name>;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO <user_name>;
ALTER DATABASE <db_name> OWNER TO <user_name>;

psql -U <user_name> -d <db_name>
```
