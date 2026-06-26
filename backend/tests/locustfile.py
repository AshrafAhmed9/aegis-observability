from locust import HttpUser, task, between


class AegisUser(HttpUser):
    wait_time = between(0.1, 0.5)
    host = "http://127.0.0.1:8001"

    @task(3)
    def ingest_redis(self):
        self.client.post("/ingest", json={"log_filename": "redis_retry_storm.log"})

    @task(2)
    def ingest_deadlock(self):
        self.client.post("/ingest", json={"log_filename": "pg_deadlock.log"})

    @task(2)
    def ingest_stampede(self):
        self.client.post("/ingest", json={"log_filename": "cache_stampede.log"})

    @task(1)
    def scenarios(self):
        self.client.get("/scenarios")

    @task(1)
    def health(self):
        self.client.get("/")
