from riffq import riffq

def handle_query(sql, callback):
    print("< received (python):", sql)

    result = (
        [ {"name": "err", "type": "str"} ],
        [ ["unknown query"] ]
    )

    callback(result)

if __name__ == "__main__":
    server = riffq.Server("127.0.0.1:5433")
    server.set_callback(handle_query)
    server.start()
