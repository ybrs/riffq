from sqlalchemy import create_engine, text
import time

def wait_for_server(port: int = 5433):
    engine = create_engine(f"postgresql://myuser:mypassword@127.0.0.1:{port}/mydb")
    import sqlalchemy.exc
    cnt = 0
    while True:
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM test_concurrency;"))
                print("connected")
                break
        except sqlalchemy.exc.OperationalError:
            print("waiting for server to be online")
            time.sleep(1)
            cnt += 1
        if cnt > 10:
            raise Exception("couldnt spawn the server")
    
    return engine

