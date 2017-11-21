import csv
import randoms
import sys

def main():
  print 'Generating lsr_data.csv...'
  num_rows = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
  with open('lsr_data.csv', 'w') as f:
    w = csv.writer(f)
    for i in range(num_rows):
      z = random.randint(1, 1000000)
      z = random.randint(1, 1000000)
      z = random.randint(1, 1000000)
      w.writerow([x, y, z])

if __name__ == '__main__':
  main()
