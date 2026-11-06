## Guide

#### 1. PostgreSQL

1.1 Connect to PostgreSQL as an administrator:
```sql
psql -U postgres
```

1.2 Create a database user and database:
```sql
-- Create a database user with a password
CREATE USER <user_name> WITH PASSWORD '<password>';

-- Create a database owned by the new user
CREATE DATABASE <db_name> OWNER <user_name>;

-- Connect to the newly created database
\c <db_name>;

-- Grant privileges on the public schema, existing tables and sequences
GRANT ALL PRIVILEGES ON SCHEMA public TO <user_name>;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO <user_name>;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO <user_name>;
```

1.3 Connect to the database using the newly created user:
```sql
psql -U <user_name> -d <db_name>
```

#### 2. Configure environment variables

2.1 Create a `.env` file and configure the required environment variables from `.env.example`.

#### 3. Alembic

3.1 Apply all pending migrations:
```bash
uv run alembic upgrade head
```

#### 4. RabbitMQ

4.1 Access RabbitMQ Management UI:
```
http://localhost:15672/
```
> Note: Log in using the credentials configured in `.env`.

4.2 Open the _**Queues and Streams**_ tab to inspect queues, messages, consumers and message processing status.

#### 5. Docker Compose

5.1 Build images and start all services:
```bash
docker compose up --build -d
```

5.2 Running migrations inside Docker:
```bash
docker compose exec api uv run alembic upgrade head
```

> Useful Docker commands:
```bash
docker compose up -d            # Starting already-built services
docker compose down             # Stopping services without deleting data
docker compose ps               # Viewing running containers
docker compose logs -f          # Viewing logs for all services 

# Connecting to PostgreSQL inside Docker
docker compose exec postgres psql -U <user_name> -d <db_name>
```
> ⚠️ Note: `docker compose down -v` removes volumes, including PostgreSQL data. Use this command only when you truly intend to delete the database data.

## Testing

#### 1. API

Create payment:
```bash
curl -X POST http://localhost:9000/api/v1/payments/ \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: ik-123-abc" \
  -d '{
    "amount": "1.00",
    "description": "text"
  }'
```
