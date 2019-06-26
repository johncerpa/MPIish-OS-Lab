import socket, errno, sys, threading, os
from time import sleep
from math import ceil
from _thread import start_new_thread
from importlib import import_module
semaphore = threading.Lock()

def Main():
  slave_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  slave_socket.connect(('127.0.0.1', 12345))
  
  # Thread is blocked until it receives some data
  data = slave_socket.recv(1024)
  inputs = data.decode('ascii')

  # Set timeout to 2s to know when server is done sending data
  slave_socket.settimeout(2)

  while data:
    try:
      data = slave_socket.recv(1024)
      inputs += data.decode('ascii')
    except socket.timeout: # Done receiving data, break loop
      break
    except socket.error as e:
      print(e)
      sys.exit(1) # 1: Abnormal termination

    
  with open('prime.py', 'w+') as f:
    start, end = "PROGRAMSTART", "PROGRAMEND"
    start_index = inputs.index(start) + len(start)
    end_index = inputs.index(end)
    f.write(inputs[start_index : end_index])

  start, end = "INPUTSTART", "INPUTEND"
  start_index = inputs.index(start) + len(start)
  end_index = inputs.index(end)
  inputs = inputs[start_index:end_index]

  # Importing program that the slave is going to run
  imported_module = __import__('prime')

  # Convert input content to integers
  complete = inputs.split(',')
  complete = [int(i) for i in complete]

  num_threads = complete[len(complete)-2]
  slave_index = complete[len(complete)-1]

  # Get numbers to check
  complete = complete[0 : len(complete)-2] 

  print('Received program.py and input successfuly\nWorking on primality test...')
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
  
  for thr in threads:
    thr.join()

  with open(f'output{slave_index}.csv', 'rb') as f:
    slave_socket.sendfile(f, 0)
  
  print('Primality test done and sent results to the server!')

if __name__ == '__main__':
  Main()