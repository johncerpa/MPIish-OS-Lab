import socket, errno, sys, threading
from time import sleep
from _thread import start_new_thread
from math import ceil
from os import remove

semaphore = threading.Lock()

def Main():
  slave_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  slave_socket.connect(('127.0.0.1', 12345))
  
  data = slave_socket.recv(1024)
  complete = data.decode('ascii')
  while data:
    try:
      slave_socket.settimeout(1) # if no more data is available, stop waiting
      data = slave_socket.recv(1024)
      complete += data.decode('ascii')
    except (socket.error, socket.timeout) as e:
      err = e.args[0]
      if err == errno.EAGAIN or err == 'timed out':  # No more data available
        break
      else:
        print(e)
        sys.exit(1)

  complete = complete.split(',')
  print(complete)
  for i in range(len(complete)):
    complete[i] = int(complete[i])

  num_threads = complete[len(complete)-2]
  slave_index = complete[len(complete)-1]

  complete = complete[0 : len(complete)-2]
  print(complete)
  per_thread = ceil(len(complete) / num_threads)

  threads = [None] * num_threads
  j = 0

  for i in range(0, len(complete), per_thread):
    threads[j] = threading.Thread(target=checkPrimes, args=(complete[i:i+per_thread], slave_index))
    threads[j].start()
    j += 1
  
  for i in range(len(threads)):
    threads[i].join()

  f = open(f'output{slave_index}.csv', 'rb')
  slave_socket.sendfile(f, 0)
  f.close()

def checkPrimes(arr, slave_index):
  result = []
  for i in range(len(arr)):
    print(isPrime(arr[i]))
    if isPrime(arr[i]):
      result.append(str(arr[i]) + ', Y\n')
    else:
      result.append(str(arr[i]) + ', N\n')
  semaphore.acquire()
  f = open(f'output{slave_index}.csv', 'a+')
  for i in range(len(result)):
    f.write(result[i])
  f.close()
  semaphore.release()


def isPrime(n):
  if n == 2 or n == 3: return True
  if n < 2 or n%2 == 0: return False
  if n < 9: return True
  if n % 3 == 0: return False
  r = int(n**0.5)
  f = 5
  while f <= r:
    if n % f == 0: return False
    if n % (f+2) == 0: return False
    f += 6
  return True  

if __name__ == '__main__':
  Main()