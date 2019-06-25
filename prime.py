def isPrime(n):
  if n == 2 or n == 3: return True
  if n < 2 or n % 2 == 0: return False
  if n < 9: return True
  if n % 3 == 0: return False
  r = int(n ** 0.5)
  f = 5
  while f <= r:
    if n % f == 0: return False
    if n % (f+2) == 0: return False
    f += 6
  return True

def checkPrimes(arr, start, end, slave_index, semaphore):
  semaphore.acquire()
  with open(f'output{slave_index}.csv', 'a+') as f:
    for i in range(start, end):
      f.write(str(arr[i]) + ', Y\n' if isPrime(arr[i]) else str(arr[i]) + ', N\n')
  semaphore.release()
