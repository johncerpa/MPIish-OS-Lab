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

def checkPrimes(arr, start, end, semaphore):
  # Reading, no problem if threads access the array at the same time
  results = []
  for i in range(start, end):
    r = 'Y' if isPrime(arr[i]) else 'N'
    results.append(f'{str(arr[i])}, {r}\n')      

  # Threads writing to the same file, a semaphore is needed
  semaphore.acquire()
  with open(f'output.csv', 'a+') as f:
    for i in range(len(results)):
      f.write(results[i]) # change to only one write
  semaphore.release()
