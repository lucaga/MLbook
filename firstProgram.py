import numpy as np
import time

def calculate_multiplication(a, b, n):
  """
  This function calculates the multiplication of the first number by the second 
  for a number of times equal to the rounded value of the third number.

  Args:
      a: The first number.
      b: The second number.
      n: The third number.

  Returns:
      A tuple containing the final result and the time taken to compute.
  """
  start_time = time.time()
  n = int(round(n))
  result = np.zeros(n)
  result[0] = a * b
  for i in range(1, n):
    result[i] = result[i-1] * b
  end_time = time.time()
  return result[-1], end_time - start_time

# Get user input
while True:
  try:
    a = float(input("Enter the first number: "))
    b = float(input("Enter the second number: "))
    n = float(input("Enter the number of times to multiply (decimal will be rounded): "))
    break
  except ValueError:
    print("Invalid input. Please enter numbers only.")

# Calculate multiplication and time
result, time_taken = calculate_multiplication(a, b, n)

# Print results
print(f"Result: {result}")
print(f"Time taken: {time_taken:.4f} seconds")
