import socket, errno, sys, threading, os
from time import sleep
from math import ceil
from _thread import start_new_thread
from importlib import import_module
semaphore = threading.Lock()

def Main():
  slave_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  slave_socket.connect(('127.0.0.1', 12345))
  
  slave_socket.settimeout(6)
  isDoneReceiving = False
  inputs = ''

  while 1:
    try:
      data = slave_socket.recv(1024)
      inputs += data.decode('ascii')
    except (socket.error, socket.timeout) as e:
      err = e.args[0]
      if err == errno.EAGAIN or err == 'timed out':  # No more data available
        isDoneReceiving = True
        break
      else:
        print(e)
        sys.exit(1)
  
  print('RECEIVED: \n', inputs)  
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

  with open('program.py', 'r') as f:
    print(f'Program:\n{f.read()}\nEnd of program\n')

  if (isDoneReceiving):
    imported_module = __import__('program')

    complete = inputs.split(',')
    for i in range(len(complete)):
      complete[i] = int(complete[i])

    num_threads = complete[len(complete)-2]
    slave_index = complete[len(complete)-1]

    # array of numbers that the threads are going to check
    complete = complete[0 : len(complete)-2] 
    per_thread = ceil(len(complete) / num_threads)
    print(complete)

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

    f = open(f'output{slave_index}.csv', 'rb')
    slave_socket.sendfile(f, 0)
    f.close()

if __name__ == '__main__':
  Main()