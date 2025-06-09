import unittest
import time
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from multiprocessing import Process
import riffq
import logging

from utils import wait_for_server
logging.basicConfig(level=logging.DEBUG)

def start_polars_server():
    from server_polars import main
    main()

def run_heavy_query():
    logging.info("sending long running query")
    engine = create_engine("postgresql://myuser:mypassword@127.0.0.1:5434/mydb")
    with engine.connect() as conn:
        # trigger the heavy Polars computation
        conn.execute(text("SELECT heavy_query"))
    logging.info("finished long running query")


class TestPolarsConcurrency(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server_proc = Process(target=start_polars_server)
        cls.server_proc.start()
        time.sleep(1.5)  # wait for server

    @classmethod
    def tearDownClass(cls):
        cls.server_proc.terminate()
        cls.server_proc.join()

    def test_concurrent_queries(self):
        engine = wait_for_server(port=5434)
        
        fast_query_times = []

        heavy_proc = Process(target=run_heavy_query)
        heavy_proc.start()

        while heavy_proc.is_alive():
            try:
                with engine.connect() as conn:
                    logging.info("executing short running query")
                    result = conn.execute(text("SELECT * FROM test_concurrency;"))
                    rows = result.mappings().all()
                    logging.info("executed short running query")
                    if rows:
                        fast_query_times.append(time.time())
            except Exception:
                pass
            time.sleep(0.3)

        heavy_proc.join()
        print("fast query times", fast_query_times)

        self.assertGreaterEqual(len(fast_query_times), 2, "Expected at least 2 successful fast queries")


if __name__ == "__main__":
    unittest.main()
