import socket, threading
from _thread import start_new_thread
from csv import reader
from math import ceil
import sys, errno

semaphore = threading.Lock()

def Main():
  num_slaves = int(input('Number of slaves: '))
  threads = int(input('Threads per slave: '))

  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('', 12345)) 
  s.listen(num_slaves)
  print('Listening on port 12345...')

  # Dividing file proportional to threads
  numbers, slaves_threads = [], []
  with open('big_input.csv') as input_file:
    csv_reader = reader(input_file, delimiter=',')
    for row in csv_reader:
      numbers.append(int(row[0]))
  per_slave = ceil(len(numbers) / num_slaves)
  inputs_list = []
  for i in range(0, len(numbers), per_slave):
    inputs_list.append(numbers[i : i + per_slave])

  for i in range(len(inputs_list)):
    with open(f'input{i}.csv', 'w+') as f:
      f.write('INPUTSTART\n')
      for j in range(len(inputs_list[i])):
        f.write(str(inputs_list[i][j]) + ',')
      f.write(str(threads) + ',' + str(i) + '\nINPUTEND\n')

  # i PROBABLY DONT HAVE TO EVEN ATTACH PROGRAMSTART, JUST SEND PROGRAMSTART DUMBBASS
  with open('prime.py', 'r+') as f:
    content = f.read()
    with open('program.py', 'w+') as p:  
      p.write('PROGRAMSTART\n' + content + '\nPROGRAMEND')

  slaves, slaves_connected = [], 0
  ready = False
  try:
    while True:
      if (slaves_connected < num_slaves):
        slave_socket, addr = s.accept()
        slaves.append(slave_socket)
        print(f'Slave connected: {addr[0]}:{addr[1]}')
        
        # Start new slave thread
        thr = threading.Thread(target=slave_thread, args=(slave_socket,))
        thr.start()
        slaves_threads.append(thr)
        slaves_connected += 1

      if (slaves_connected == num_slaves):
        ready = True
        slaves_connected += 1

      if (ready):
        print('Inputs divided: ', inputs_list)
        for i in range(len(inputs_list)): # Some slaves might not have a job
          print(f'Sending files to slave {i}')
          thr = threading.Thread(
            target=send_files,
            args=(i, slaves)
          )
          thr.start()

        ready = False
        print('Sent files to every slave')

  except KeyboardInterrupt:
    s.close()
    for slave in slaves:
      slave.close()

def send_files(i, slaves):
  with open(f'input{i}.csv', 'rb') as f:
    slaves[i].sendfile(f, 0)

  with open('program.py', 'rb') as f: 
    slaves[i].sendfile(f, 0)

def slave_thread(slave_socket):
  # blocking without timeout, the rest runs after this is done
  data = slave_socket.recv(1024)
  complete = data.decode('ascii')
  while data:
    try:
      slave_socket.settimeout(4) # if no more data is available, stop waiting
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
  with open('results.csv', 'a+') as f:
    f.write(complete)
  semaphore.release()

  slave_socket.close()

if __name__ == '__main__':
  Main()