import socket, errno, sys, threading, os
from time import sleep
from math import ceil
from _thread import start_new_thread
from importlib import import_module
semaphore = threading.Lock()

def Main():
  slave_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  slave_socket.connect(('127.0.0.1', 12345))
  
  # Sets a limit of 100s to receive the first bytes
  slave_socket.settimeout(100)

  isTimeoutSet, inputs = False, ''

  while 1:
    try:
      data = slave_socket.recv(1024)
      inputs += data.decode('ascii')

      # Set a lower limit to know when is done receiving
      if (not isTimeoutSet):
        slave_socket.settimeout(2)
        isTimeoutSet = True
        print('Receiving files...')

    except (socket.error, socket.timeout) as e:
      err = e.args[0]
      if err == errno.EAGAIN or err == 'timed out':
        break
      else:
        print(e)
        sys.exit(1)
    
  with open('program.py', 'w+') as f:
    start, end = "PROGRAMSTART", "PROGRAMEND"
    start_index = inputs.index(start) + len(start)
    end_index = inputs.index(end)
    content = inputs[start_index : end_index]
    f.write(content)

  start, end = "INPUTSTART", "INPUTEND"
  start_index = inputs.index(start) + len(start)
  end_index = inputs.index(end)
  inputs = inputs[start_index:end_index]

  print('Received program.py and input successfuly')

  # Importing program that the slave is going to run
  imported_module = __import__('program')

  # Convert input content to integers
  complete = inputs.split(',')
  complete = [int(i) for i in complete]

  num_threads = complete[len(complete)-2]
  slave_index = complete[len(complete)-1]

  # Get numbers to check
  complete = complete[0 : len(complete)-2] 
  print(f'Primality test for array: {complete}')

  per_thread = ceil(len(complete) / num_threads)
  threads = []
  for i in range(0, len(complete), per_thread):
    end = i + per_thread
    if (end > len(complete)):
      end = len(complete)
      
    thr = threading.Thread(
      target=imported_module.checkPrimes,
      args=(complete, i, end, slave_index, semaphore)
    )
    thr.start()
    threads.append(thr)

    print(f'Thread checking: {complete[i : end]}')
  
  for thr in threads:
    thr.join()

  with open(f'output{slave_index}.csv', 'rb') as f:
    slave_socket.sendfile(f, 0)

if __name__ == '__main__':
  Main()