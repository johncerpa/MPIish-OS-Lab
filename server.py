import socket, threading
from _thread import start_new_thread
from csv import reader
from math import ceil
import sys, errno

semaphore = threading.Lock()
done = False

def Main():
  num_slaves = int(input('Number of slaves: '))
  threads = int(input('Threads per slave: '))

  slaves_threads = []

  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('', 12345))
  s.listen(num_slaves)
  print('Server listening on port 12345...')

  # Dividing file proportional to threads
  numbers = []
  with open('big_input.csv') as input_file:
    csv_reader = reader(input_file, delimiter=',')
    for row in csv_reader:
      numbers.append(int(row[0]))
  per_slave = ceil(len(numbers) / num_slaves)
  inputs_list = []
  for i in range(0, len(numbers), per_slave):
    inputs_list.append(numbers[i : i + per_slave])

  slaves, slaves_connected = [], 0
  ready = False
  try:
    while True:
      if (slaves_connected < num_slaves):
        
        slave_socket, addr = s.accept()
        slaves.append(slave_socket)
        print(f'Slave connected: {addr[0]}:{addr[1]}')
        
        # Start new slave thread
        thr = threading.Thread(target=slave_thread, args=(slave_socket, slaves_connected))
        thr.start()
        slaves_threads.append(thr)

        slaves_connected += 1

      if (slaves_connected == num_slaves):
        ready = True
        slaves_connected += 1 # so it only does this once

      if (ready):
        print('All slaves are connected')
        for i in range(len(inputs_list)): # Some slaves might not have a job
          
          # Creating a file for each slave
          with open(f'input{i}.csv', 'w+') as f: 
            for j in range(len(inputs_list[i])):
              f.write(str(inputs_list[i][j]) + ',')
            # Attach number of threads and slave index
            f.write(str(threads) + ',' + str(i))

          # Sending file with input numbers to each slave
          slaves[i].send(b'input')
          with open(f'input{i}.csv', 'rb') as f:
            slaves[i].sendfile(f, 0)

          # Send program.py
          slaves[i].send(b'program')
          with open('prime.py', 'rb') as f: 
            slaves[i].sendfile(f, 0)
          slaves[i].send(b'program_done')
          
        ready = False # only do it once
        print('Sent files to every slave')        

  except KeyboardInterrupt:
    s.close()
    for slave in slaves:
      slave.close()

def slave_thread(slave_socket, index):
  data = slave_socket.recv(1024)
  complete = data.decode('ascii')
  while data:
    try:
      slave_socket.settimeout(2) # if no more data is available, stop waiting
      data = slave_socket.recv(1024)
      complete += data.decode('ascii')
    except (socket.error, socket.timeout) as e:
      err = e.args[0]
      if err == errno.EAGAIN or err == 'timed out':  # No more data available
        break
      else:
        print(e)
        sys.exit(1)

  semaphore.acquire()
  f = open(f'results.csv', 'a+')
  f.write(complete)
  f.close()
  semaphore.release()
  slave_socket.close()

if __name__ == '__main__':
  Main()












