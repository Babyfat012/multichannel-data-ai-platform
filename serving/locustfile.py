"""Load test Web API feature, xuất HTML.

locust -f serving/locustfile.py --host http://localhost:8000 --headless -u 20 -r 5 -t 60s \
    --html out/load-test.html
"""

import os
import random

from locust import HttpUser, between, task

SKUS = os.environ.get("LOAD_SKUS", "sku-A,sku-B").split(",")


class FeatureUser(HttpUser):
    wait_time = between(0.05, 0.2)

    @task(4)
    def one_sku(self):
        self.client.post("/features", json={"seller_skus": [random.choice(SKUS)]})

    @task(1)
    def batch(self):
        self.client.post("/features", json={"seller_skus": SKUS * 5}, name="/features [batch]")

    @task(1)
    def health(self):
        self.client.get("/healthz")
