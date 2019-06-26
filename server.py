import socket, threading, file_handling, sys, time

semaphore = threading.Lock()
slaves_on = 0

def Main():
  num_slaves = int(input('Number of slaves: '))
  threads = int(input('Threads per slave: '))

  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('', 12345)) 
  s.listen(num_slaves)
  print('Listening on port 12345...')

  inputs_list = file_handling.divideInputFile('big_input.csv', num_slaves)
  file_handling.saveInputFiles(threads)

  t = None

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
 
      # If all slaves required are connected
      if (ready): 
        t = time.time()
        for i in range(len(inputs_list)):
          print(f'Sending files to slave {i}')
          thr = threading.Thread(
            target=file_handling.send_files,
            args=(i, slaves)
          )
          thr.start()
        ready = False
        print('Sent files to every slave')
      
      if (slaves_on <= 0):
        print(f'All the slaves are done with their jobs\nTime taken: {time.time() - t} seconds\nServer is exiting...')
        s.close()
        break

  except KeyboardInterrupt:
    s.close()
    for slave in slaves:
      slave.close()

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