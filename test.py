from typing import List, Tuple
import zmq
import random
import pprint
from concurrent.futures import ProcessPoolExecutor
import time
import numpy as np
def producer():
    context = zmq.Context()
    
    # zmq_socket.bind("tcp://127.0.0.1:5557")
    socks = []
    for i in range(10):
        zmq_socket = context.socket(zmq.PUSH)
        zmq_socket.bind(f"ipc://{i}")
        # zmq_socket.bind(f"tcp://0.0.0.0:999{i}")
        socks.append((zmq_socket, i))

    
    # Start your result manager and workers before you start your producers
    sock = (None,None)
    time.sleep(1)
    for num in range(100):
        try:
            time.sleep(1.01)
            # sock =random.choice([b for b in socks[:2] if b[0] != sock[0]])
            sock =random.choice([b for b in socks[:2]])
            # sock = socks[0]
            work_message = { 'num' : num, 'start':time.time(), 'dat': np.zeros((10,10)).tolist() , 'sock': sock[1]}
            s = time.time()
            sock[0].send_json(work_message)
            print(f'send {num} {sock[1]} {int(time.time())%100}')
        except Exception as e:
            import traceback as tb
            print(tb.format_exc())


def consumer(i):
    try:
        consumer_id = i
        print("I am consumer #%s" % (consumer_id))
        context = zmq.Context()
        # recieve work
        socks : List[Tuple[zmq.Socket, int]] = []
        poller = zmq.Poller()
        for i in range(10):
            consumer_receiver = context.socket(zmq.PULL)
            # consumer_receiver.connect("tcp://127.0.0.1:5557")
            consumer_receiver.connect(f"ipc://{i}")
            # consumer_receiver.connect(f"tcp://0.0.0.0:999{i}")
            poller.register(consumer_receiver, zmq.POLLIN)
            socks.append((consumer_receiver,i))
        
            
        # send work
        consumer_sender = context.socket(zmq.PUSH)
        consumer_sender.connect("tcp://127.0.0.1:5558")
        
        while True:
            s = time.time()
            events = poller.poll()
            # print(f'poll time {(time.time() - s):.4f}')
            for e in events:
                work = e[0].recv_json()
                result = { 'consumer' : consumer_id, 'num' : work['num'], 'pending': int((time.time()- work['start'])*1000), 'sock': work['sock']}
                print(f'consume {result} {int(time.time())%100} ')
                time.sleep(2)
                consumer_sender.send_json(result)
                # break
    except Exception as e:
        import traceback as tb

        print(tb.format_exc())

def result_collector():
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    results_receiver.bind("tcp://127.0.0.1:5558")
    collecter_data = {}
    print('collector started')
    pending =[]
    sum = 0
    count = 0
    try:
        for x in range(100):
            result = results_receiver.recv_json()
            
            if result['sock'] in collecter_data:
                collecter_data[result['sock']] = collecter_data[result['sock']] + 1
            else:
                collecter_data[result['sock']] = 1
            sum += result['pending']
            count += 1
            # print(f'collector {collecter_data} {sum/count}')
    except Exception as e:
        print(e)


if __name__ == '__main__':
    pool = ProcessPoolExecutor(8)

    pool.submit(producer)
    pool.submit(result_collector)
    for i in range(2):
        pool.submit(consumer, i)
    
    pool.shutdown()