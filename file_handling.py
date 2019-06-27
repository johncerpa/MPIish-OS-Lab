from math import ceil
from csv import reader 

inputs_list = []

def divideInputFile(name, num_slaves):
  numbers = []

  with open(name) as input_file:
    csv_reader = reader(input_file, delimiter=',')
    for row in csv_reader:
      numbers.append(int(row[0]))
  
  per_slave = ceil(len(numbers) / num_slaves)
  
  for i in range(0, len(numbers), per_slave):
    inputs_list.append(numbers[i : i + per_slave])
    
  return inputs_list

def saveInputFiles(threads):
  arr = []
  for i in range(len(inputs_list)):
    myStr = ''
    for j in range(len(inputs_list[i])):
      if (j == len(inputs_list[i])-1):
        myStr += str(inputs_list[i][j]) # No comma at the end
      else:  
        myStr += str(inputs_list[i][j]) + ','
    arr.append(myStr)

  for i in range(len(arr)):
      with open(f'input{i}.csv', 'w+') as f:
        f.write(f'{arr[i]},{str(threads)}\n')

def send_files(i, slaves):
  slaves[i].send(b'INPUTSTART')
  with open(f'input{i}.csv', 'rb') as f:
    slaves[i].sendfile(f, 0)
  slaves[i].send(b'INPUTEND')

  slaves[i].send(b'PROGRAMSTART')
  with open('prime.py', 'rb') as f: 
    slaves[i].sendfile(f, 0)
  slaves[i].send(b'PROGRAMEND')