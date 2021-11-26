from multiprocessing import Process, Event
import numpy as np
from numpy.random import default_rng
import time
from util import current_milli_time, get_next_noisy_time
import socket
import pickle


class Base:
    def __init__(self, host, port, vector_size, packets_per_second):
        self.address = (host, port)
        self.vector_size = vector_size
        self.pckts_per_sec = packets_per_second
        self.delta_t = 1 / packets_per_second


class Consumer(Base):
    def __init__(self, host, port, vector_size, packets_per_second):
        super().__init__(host, port, vector_size, packets_per_second)
        self.msg_received = 0
        self.buf_size = 1024
        self.data_acq_rates = []
        self.data_acq_mean = []
        self.data_acq_std = []

    def run(self, event):
        self.event = event
        print('running server')
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind(self.address)
        self.s.listen(1)
        print('listening..')
        self.conn, self.addr = self.s.accept()
        self.conn.settimeout(0.001)

        self.consume()

    def receive_exactly(self, n):
        data = b''

        while n > 0:
            try:
                chunk = self.conn.recv(n)
                n -= len(chunk)
                data += chunk
            except socket.timeout as e:
                if not self.event.is_set():
                    print('socket timed out due to noisy_mode')

                if len(data) > 0:
                    print(f'data={data}')
                    raise e
                return None

        return data

    def consume(self):
        self.pre_time = time.time()
        while True:
            if not self.event.is_set():
                print('warning! a packet was lost')
                s = time.time()
                self.event.wait()
                print(
                    f'waited for event {round(time.time() - s, 2)} secs, self.msg_received = {self.msg_received}')
                continue
            # data_vector = self.conn.recv(self.buf_size)
            # header = self.receive_exactly(8)
            # size = struct.unpack("!Q", header)[0]
            # print(f'size={size}')
            data = self.receive_exactly(550)
            if data is not None:
                data_vector = pickle.loads(data)
                self.msg_received += 1
            if self.msg_received % self.pckts_per_sec == 0:
                end_time = time.time()
                tot_time = end_time - self.pre_time
                print(f'tot_time={round(tot_time, 4)}')
                data_acqus_rate = round(self.pckts_per_sec / tot_time)
                self.pre_time = time.time()
                print(f'data_acqus_rate:{data_acqus_rate}Hz')
                self.data_acq_rates.append(data_acqus_rate)
                if len(self.data_acq_rates) == 5:
                    np_rates = np.array(self.data_acq_rates)
                    print(f'rates={np_rates}')
                    print(f'mean={np.mean(np_rates)}')
                    print(f'std={round(np.std(np_rates), 2)}')


class Producer(Base):

    def run(self, event):
        self.event = event
        event.set()
        print('running producer')
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.settimeout(5)
        self.s.connect(self.address)
        self.produce()

    def is_noisy(self, next_noisy_time):
        current_time = current_milli_time()
        adjusted_noisy_time = next_noisy_time
        if current_time >= adjusted_noisy_time:
            print(
                f'next={next_noisy_time}, current_time={current_time}, adujsted={adjusted_noisy_time}')
            return True
        else:
            return False

    def produce(self):
        next_noisy_time = get_next_noisy_time()
        # print(f'next_noisy={next_noisy_time}')
        packet_lose = [time.time()]

        try:
            while True:
                pre_sending_time = time.time()
                if not self.event.is_set():
                    self.event.set()
                    print('event cleared')
                for i in range(1, self.pckts_per_sec):
                    if self.is_noisy(next_noisy_time):
                        print('is noisy is True!')
                        self.event.clear()
                        packet_lose.append(time.time())
                        if len(packet_lose) == 10:
                            for i in range(0, 9):
                                print(
                                    f'interval={round(packet_lose[i + 1] - packet_lose[i], 4)}')
                        next_noisy_time = get_next_noisy_time(next_noisy_time)
                        time.sleep(0.0015)
                        self.event.set()
                    rand_vector = np.random.uniform(-1, 0,
                                                    self.vector_size)
                    data_vector = pickle.dumps(rand_vector)
                    self.s.sendall(data_vector)
                pause_for = self.delta_t * self.pckts_per_sec - (
                            time.time() - pre_sending_time)
                pause_for = 0 if pause_for < 0 else pause_for
                # print(f'pre_time:{pre_sending_time}, delta_t={self.delta_t}, pause={pause_for}')
                time.sleep(pause_for)
                tot_time = time.time() - pre_sending_time
                # print(f'send rate={round(1000/ tot_time)}Hz')
        except BaseException as e:
            print(f'Exception was thrown on the producer:\n{e}')
            raise e
        finally:
            self.s.close()


if __name__ == "__main__":
    host = 'localhost'
    port = 8008
    event = Event()
    event.set()
    vector_size = 50
    packets_for_sec = 1000
    producer = Producer(host, port, vector_size, packets_for_sec)
    consumer = Consumer(host, port, vector_size, packets_for_sec)
    proc_consumer = Process(target=consumer.run, args=(event,))
    proc_producer = Process(target=producer.run, args=(event,))
    proc_consumer.start()
    proc_producer.start()
    print('both objects created')

    proc_consumer.join()
    proc_producer.join()
    """
    producer.start()
    consumer.start()
    producer.join()
    consumer.join()
    """
