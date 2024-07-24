"""
This module defines a simple function to add two numbers
and demonstrates its usage with some variables.
"""

# Define a function named 'add' that takes two arguments, 'number1' and 'number2'.
# The purpose of this function is to add the two numbers and return the result.
def add(number1, number2):
    """
    Add two numbers and return the result.

    Args:
        number1 (int or float): The first number to add.
        number2 (int or float): The second number to add.

    Returns:
        int or float: The sum of number1 and number2.
    """
    return number1 + number2

# Initialize the constant variable 'NUM1' with the value 4.
# Constants are usually written in uppercase letters to indicate that they should not be changed.
NUM1 = 4

# Initialize the variable 'NUM2' with the value 5.
# This variable will be used as the second input to the 'add' function.
NUM2 = 5

# Call the 'add' function with 'NUM1' and 'NUM2' as arguments.
# The result of this addition operation is stored in the variable 'total'.
TOTAL = add(NUM1, NUM2)

# Print a formatted string that displays the sum of 'NUM1' and 'NUM2'.
# Using an f-string to insert the values of 'NUM1', 'NUM2', and 'TOTAL' into the string.
print(f"The sum of {NUM1} and {NUM2} is {TOTAL}")
