from riffq import riffq

def run_query(query):
    return [{"err": "str"}], [["unknown query"]]

def handle_query(sql):
    print("< received (python):", sql)
    return (
        [ {"name": "err", "type": "str"} ],
        [ ["unknown query"] ]
    )

if __name__ == "__main__":
    server = riffq.Server("127.0.0.1:5433")
    server.set_callback(handle_query)
    server.start()