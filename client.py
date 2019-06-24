import socket, errno, sys, threading, os
from time import sleep
from math import ceil
from _thread import start_new_thread
from importlib import import_module
semaphore = threading.Lock()

def Main():
  isDoneReceiving = False
  slave_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  slave_socket.connect(('127.0.0.1', 12345))
  
  data = slave_socket.recv(1024)
  decoded = data.decode('ascii')
  inputs, receiving = '', ''
  with open('program.py', 'w+') as f: 
    while data:
      try:
        slave_socket.settimeout(2)
        
        if (decoded == 'input'):
          receiving = 'input'
          decoded = ''
          print('Receiving input...')
          data = slave_socket.recv(1024)
          decoded = data.decode('ascii')
        
        if (decoded == 'program'):
          receiving = 'program'
          decoded = ''
          print('Receiving program...')
          data = slave_socket.recv(1024)
          decoded = data.decode('ascii')

        if (decoded == 'program_done'):
          receiving = ''
          decoded = ''
          isDoneReceiving = True
          break

        if (receiving == 'input'):
          inputs += decoded
        if (receiving == 'program' and decoded != 'program_done'):
          f.write(decoded)

        data = slave_socket.recv(1024)
        decoded = data.decode('ascii')

      except (socket.error, socket.timeout) as e:
        err = e.args[0]
        if err == errno.EAGAIN or err == 'timed out':  # No more data available
          break
        else:
          print(e)
          sys.exit(1)
  
  print(f'Input: \n {inputs}\nEnd of input\n')
  with open('program.py', 'r') as f:
    print(f'Program:\n{f.read()}\nEnd of program\n')

  # import program.py and job for slaves
  #exec('from program import checkPrimes') in globals()
  if (isDoneReceiving):
    imported_module = __import__('program')

    complete = inputs.split(',')
    for i in range(len(complete)):
      complete[i] = int(complete[i])

    num_threads = complete[len(complete)-2]
    slave_index = complete[len(complete)-1]

    complete = complete[0 : len(complete)-2]
    per_thread = ceil(len(complete) / num_threads)
    print(complete)

    threads = [None] * num_threads
    j = 0
    for i in range(0, len(complete), per_thread):
      threads[j] = threading.Thread(target=imported_module.checkPrimes, args=(complete[i:i+per_thread], slave_index, semaphore))
      threads[j].start()
      j += 1
    
    for i in range(len(threads)):
      threads[i].join()

    f = open(f'output{slave_index}.csv', 'rb')
    slave_socket.sendfile(f, 0)
    f.close()

if __name__ == '__main__':
  Main()