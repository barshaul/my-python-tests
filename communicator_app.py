import csv
import numpy as np
import pickle
import time
import socket
import sys

from multiprocessing import Process, Event
from util import current_milli_time


class Base:
    """
    Base class for both the consumer and producer classes
    """
    def __init__(self, host, port, timeout, vector_size, packets_per_second):
        self.address = (host, port)
        self.timeout = timeout
        self.vector_size = vector_size
        self.pckts_per_sec = packets_per_second
        self.delta_t = 1 / packets_per_second


class Consumer(Base):
    """
    A Consumer class to consume random data vectors from a producer over the
    socket
    """
    def __init__(self, host, port, timeout, vector_size, packets_per_second,
                 buffer_size=1024, matrix_size=100):
        super().__init__(host, port, timeout, vector_size, packets_per_second)
        self.vectors_received = 0
        self.buf_size = buffer_size
        self.matrix_size = matrix_size
        self.data_acq_rates = []
        self.data_acq_mean = []
        self.data_acq_std = []
        self.data_matrices = []
        rand_vector = np.random.uniform(0, 0, vector_size)
        self.data_size = len(pickle.dumps(rand_vector))

    def run(self, network_is_up):
        """
        Start the Consumer
        """
        self.network_is_up = network_is_up
        print('Running Consumer')
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(self.address)
        self.socket.listen(1)
        print('Consumer: listening..')
        self.conn, self.addr = self.socket.accept()
        self.conn.settimeout(0.001)

        self.consume()
        self.analyze_data()
        self.save_to_file()

    def receive_exactly(self, n):
        """
        Receive exactly {n} bytes and return them.
        If socket was timed out and no data was available, None will be
        returned.
        """
        data = b''
        while n > 0:
            try:
                chunk = self.conn.recv(n)
                n -= len(chunk)
                data += chunk
            except socket.timeout as e:
                if len(data) > 0:
                    print(f'The consumer\'s socket timed out while receiving data')
                    raise e
                return None

        return data

    def consume(self):
        """
        Consume data vectors from the producer
        """
        start_time = time.time()
        deadline = start_time + self.timeout
        curr_matrix = np.zeros((self.vector_size, self.matrix_size))
        try:
            while time.time() < deadline:
                if not self.network_is_up.is_set():
                    # Noisy mode is on
                    print('Warning! There was a packet lost')
                    s = time.time()
                    # Wait until the noisy mode ends
                    self.network_is_up.wait()
                # Receive the exact bytes sent as part of the vector
                data = self.receive_exactly(self.data_size)
                if data is None:
                    # Nothing to do
                    continue
                # Unpickle the vector
                data_vector = pickle.loads(data)
                # Calculate the index of the current vector in the matrix
                vector_idx = self.vectors_received % self.matrix_size
                # Add the vector to the current matrix
                curr_matrix[:, vector_idx] = data_vector
                self.vectors_received += 1
                if self.vectors_received % self.matrix_size == 0:
                    # Save the current matrix to the matrices array
                    self.data_matrices.append(curr_matrix)
                    # Create a new matrix
                    curr_matrix = np.zeros((self.vector_size, self.matrix_size))
                if self.vectors_received % self.pckts_per_sec == 0:
                    # Calculate the data acquisition rate of the last
                    # {self.pckts_per_sec} vectors
                    time_spent = time.time() - start_time
                    data_acqus_rate = round(self.pckts_per_sec / time_spent)
                    # reset the start_time for the next series of vectors
                    start_time = time.time()
                    print(f'Data acquisition rate={data_acqus_rate}Hz')
                    # Add the current rate to the rates array
                    self.data_acq_rates.append(data_acqus_rate)

        except BaseException as e:
            print(f'Exception was thrown on the consumer:\n{e}')
            raise e
        finally:
            self.socket.close()

    def analyze_data(self):
        # Data acquisition rates
        np_rates = np.array(self.data_acq_rates)
        self.rates_mean = round(np.mean(np_rates), 2)
        self.rates_std = round(np.std(np_rates), 2)
        print(f'rates={np_rates}')
        print(f'rates_mean={np.mean(np_rates)}')
        print(f'rates_std={round(np.std(np_rates), 2)}')
        # Data metrices
        self.metrices_mean = [np.round(np.mean(matrix, 0), 3) for matrix in self.data_matrices]
        self.metrices_std = [np.round(np.std(matrix, 0), 3) for matrix in self.data_matrices]
        x = 6

    def save_to_file(self):
        """
        Save results to file
        """
        time_str = time.strftime("%Y%m%d-%H%M%S")
        file_name = f'results_{time_str}.csv'
        with open(file_name, 'w', newline='') as f:
            writer = csv.writer(f)
            f.write("Data Acquisition Rates:\n")
            writer.writerow(self.data_acq_rates)
            f.write("\nData Acquisition Rates - Mean:\n")
            f.write(f'{self.rates_mean}\n')
            f.write("\nData Acquisition Rates - Standard Deviation:\n")
            f.write(f'{self.rates_std}\n')
            f.write("\nData Analytics Results - Mean:\n")
            writer.writerows(self.metrices_mean)
            f.write("\nData Analytics Results - Standard Deviation:\n")
            writer.writerows(self.metrices_std)


class Producer(Base):
    """
    Producer class for generating random data vector and communicating them
    to consumers
    """
    def run(self, network_is_up):
        """
        Start the Producer
        """
        self.network_is_up = network_is_up
        print('Running Producer')
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(5)
        self.socket.connect(self.address)
        self.network_is_up.set()
        self.produce()

    def is_noisy(self, next_noisy_time):
        """
        Check if the current time needs to be set as a noisy time
        """
        current_time = current_milli_time()
        adjusted_noisy_time = next_noisy_time
        return current_time >= adjusted_noisy_time

    def get_next_noisy_time(self, prev_noisy_time=0):
        """
        Get the next noisy time based on a random interval of [2, 3] seconds
        """
        if prev_noisy_time == 0:
            prev_noisy_time = current_milli_time()
        interval = np.random.uniform(2000, 3001)
        next_noisy = prev_noisy_time + interval
        return round(next_noisy)

    def produce(self):
        """
        Produce random data vectors and send them across the socket
        """
        deadline = time.time() + self.timeout
        # Initialize the next noisy time
        next_noisy_time = self.get_next_noisy_time()
        try:
            while time.time() < deadline:
                start_time = time.time()
                for i in range(1, self.pckts_per_sec):
                    if self.is_noisy(next_noisy_time):
                        # Clear the network_is_up event to notify on a noisy
                        # mode
                        self.network_is_up.clear()
                        # Calculate the next noisy time
                        next_noisy_time = self.get_next_noisy_time(next_noisy_time)
                        # Sleep to stimulate the time spent on trying to send
                        # the message
                        time.sleep(0.0015)
                        # Set back the network to True
                        self.network_is_up.set()
                        continue
                    rand_vector = np.random.uniform(-1, 0,
                                                    self.vector_size)
                    data_vector = pickle.dumps(rand_vector)
                    self.socket.sendall(data_vector)
                # Adjust the required pause based on the time spent sending the
                # vectors
                time_spent = time.time() - start_time
                pause_for = self.delta_t * self.pckts_per_sec - time_spent
                pause_for = 0 if pause_for < 0 else pause_for
                time.sleep(pause_for)
        except BaseException as e:
            print(f'Exception was thrown on the producer:\n{e}')
            raise e
        finally:
            self.socket.close()


if __name__ == "__main__":
    try:
        timeout = int(sys.argv[1])
    except IndexError:
        print("Please specify timeout in seconds. Usage example:\n"
              "python ./communicator_app.py 60")
        sys.exit(1)
    if timeout <= 0:
        print("Please pass a valid timeout >= 0")
        sys.exit(1)

    host = 'localhost'
    port = 8008
    network_is_up = Event()
    vector_size = 50
    packets_for_sec = 1000
    producer = Producer(host, port, timeout, vector_size, packets_for_sec)
    consumer = Consumer(host, port, timeout, vector_size, packets_for_sec)
    proc_consumer = Process(target=consumer.run, args=(network_is_up,))
    proc_producer = Process(target=producer.run, args=(network_is_up,))
    # Start a new process for each object (consumer / producer)
    proc_consumer.start()
    proc_producer.start()
    proc_consumer.join()
    proc_producer.join()

