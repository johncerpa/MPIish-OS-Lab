import socket, threading, prepare, sys

semaphore = threading.Lock()
slaves_on = 0

def Main():
  num_slaves = int(input('Number of slaves: '))
  threads = int(input('Threads per slave: '))

  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('', 12345)) 
  s.listen(num_slaves)
  print('Listening on port 12345...')

  inputs_list = prepare.divideInputFile('big_input.csv', num_slaves)
  prepare.saveInputFiles(threads)

  slaves, slaves_threads, ready, slaves_connected = [], [], False, 0
  try:
    while True:
      global slaves_on
      if (slaves_connected < num_slaves):
        slave_socket, addr = s.accept()
        slaves.append(slave_socket)
        print(f'Slave connected: {addr[0]}:{addr[1]}')
        
        # Start new slave thread
        thr = threading.Thread(target=slave_thread, args=(slave_socket,))
        thr.start()
        slaves_threads.append(thr)

        slaves_connected += 1
        slaves_on = slaves_connected

      if (slaves_connected == num_slaves):
        ready = True
        slaves_connected += 1

      if (ready):
        for i in range(len(inputs_list)): # Some slaves might not have a job
          print(f'Sending files to slave {i}')
          thr = threading.Thread(
            target=send_files,
            args=(i, slaves)
          )
          thr.start()
        ready = False
        print('Sent files to every slave')
      
      if (slaves_on <= 0):
        print('All the slaves are done with their jobs, exiting now!')
        s.close()
        break

  except KeyboardInterrupt:
    s.close()
    for slave in slaves:
      slave.close()

def send_files(i, slaves):
  slaves[i].send(b'INPUTSTART')
  with open(f'input{i}.csv', 'rb') as f:
    slaves[i].sendfile(f, 0)
  slaves[i].send(b'INPUTEND')

  slaves[i].send(b'PROGRAMSTART')
  with open('prime.py', 'rb') as f: 
    slaves[i].sendfile(f, 0)
  slaves[i].send(b'PROGRAMEND')

def slave_thread(slave_socket):
  data = slave_socket.recv(1024)
  complete = data.decode('ascii')

  slave_socket.settimeout(4)

  while data:
    try:
      data = slave_socket.recv(1024)
      complete += data.decode('ascii')
    except socket.timeout:
      break
    except socket.error as e:
      print(e)
      sys.exit(1)
    
  semaphore.acquire()
  with open('results.csv', 'a+') as f:
    f.write(complete)
  semaphore.release()

  slave_socket.close()

  global slaves_on
  slaves_on -= 1

if __name__ == '__main__':
  Main()