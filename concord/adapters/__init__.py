"""
Production adapters. Each lazy-imports its driver so that installing Concord for
tests or the benchmark does not drag in psycopg, confluent-kafka, redis, and
minio. Turn them on with the extras: `pip install concord[prod]`.
"""
