import socket, threading, file_handling, sys, time

semaphore = threading.Lock()
slaves_on = 0

def Main():
  
  port = 12345
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('', port)) 
  s.listen(100)
  print(f'NOTE: if you want to add more slaves you should answer \'n\'. \nIf you answer \'y\' then the slaves that connected after are going to be ignored.\nServer is listening on port {port}...')

  t = None
  shouldStart, isReady = False, False
  slaves, slaves_threads = [], []
  
  try:
    while True:
      
      global slaves_on

      if (not shouldStart):
        print('Waiting for a slave to connect...')  
        try:      
          slave_socket, addr = s.accept()
          slaves.append(slave_socket)
          slaves_on += 1
          print(f'Slave connected: {addr[0]}:{addr[1]}')
        except socket.error:
          print('There was an error connecting to a slave')
          pass
        
        ans = input('Do you want to add mores slaves after this connection? (y/n) >> ')
        if (ans == 'n'):
          shouldStart = True
          isReady = True

      if (isReady):
        # If all slaves required are connected
        inputs_list = file_handling.divideInputFile('big_input.csv', slaves_on)
        file_handling.saveInputFiles()

        threads = int(input('Threads per slave: '))

        t = time.time()
        for i in range(len(inputs_list)):
          print(f'Sending files to slave {i}')
          thr = threading.Thread(
            target=slave_thread,
            args=(i, slaves, threads)
          )
          thr.start()
          slaves_threads.append(thr)
        
        # Close connection of slaves that don't have a job to do
        if (len(inputs_list) < slaves_on):
          for i in range(len(inputs_list), slaves_on):
            slaves[i].send(b'close')
            print(f'There is no inputs left for slave {i}. Shutting it down...')
          slaves_on = len(inputs_list)

        isReady = False
        print('Sent files to every slave')
      
      if (slaves_on <= 0):
        print(f'All the slaves are done with their jobs\nTime taken: {time.time() - t} seconds\nServer is exiting...')
        s.close()
        break

  except KeyboardInterrupt:
      s.close()
      for slave in slaves:
        slave.close()

def slave_thread(i, slaves, threads):

  slave_socket = slaves[i]
  file_handling.send_files(i, slaves, threads)

  data = slave_socket.recv(1024)
  complete = data.decode('ascii')
  slave_socket.settimeout(1)

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