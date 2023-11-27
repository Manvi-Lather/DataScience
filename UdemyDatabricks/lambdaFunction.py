# Databricks notebook source
# https://www.youtube.com/watch?v=P8MdDCTbMOI

# COMMAND ----------

# lambda function is an anonymous function defined without a name and without using def keyword

# a lambda function can take any number of arguments but can only have one expression 

# COMMAND ----------

def add(a,b): 
    return a + b

add(3,4)

# COMMAND ----------

# lambda function


add = lambda a,b: a+b

add(3,4)

# COMMAND ----------

def add(a,b): 
    result = a+b 
    return result

add(1,2)

# COMMAND ----------

add = lambda a,b : a+b
add(2,3)

# COMMAND ----------

add = lambda x: x+100

add(40)

# COMMAND ----------

(lambda x: x+100) (40)

# COMMAND ----------

# -- no need to explicitly assign a variable to a lambda 

(lambda a,b : a*b)(16,2)

# COMMAND ----------

product = lambda x , y,z : x*y*z

print(product(z= 5, x= 10, y=4))

# COMMAND ----------

add = lambda x , y = 15, z = 24 : x + y +z
print(add(x=7))  # I will be giving just x value as for y and z , I have already declared the value 

# COMMAND ----------

# using args allows me to define as many arguments as I want  

test = lambda *args: sum(args)
result = test(20, 2, 3, 40)
print(result)

# COMMAND ----------

higher_ord_fun = lambda x , fun : x + fun(x)

higher_ord_fun(20, lambda x: x *x)

# COMMAND ----------

(lambda x : (x %2 and 'odd' or 'even'))(150)

# COMMAND ----------

#check if substring is in a string 


sub_string = lambda string : string in "welcome to python function tutorial"


print(sub_string('python'))

print(sub_string('java'))

# COMMAND ----------

#  lambda function using filter function 


num = [10,40,56,27,33,15,70]

greater = list(filter(lambda num : num > 30, num))

greater

# COMMAND ----------

# list of functions divisible by 4 


num = [10,40,56,27,33,15,70]
 
div_four = list(filter((lambda x : x %4 == 0),num))

div_four

# COMMAND ----------

#map function : 

list1= [10,40,56,27,33,15,70]

double_the_num = list(map(lambda x: x*2, list1))
double_the_num

# COMMAND ----------

#power of the number

list2= [2,3,4,5]

cube = list(map(lambda x :pow(x,3),list2))

cube

# COMMAND ----------

# reduce   --> apply function to an iterable and reduce it to a single cumulative value . 

# performs function on first two elements and repeat process untill 1 value remains 

#sum of all the num in the list 
from functools import reduce

list4 = [2,3,4]

sum_all = reduce((lambda x,y: x +y),list4)

sum_all

# COMMAND ----------

letters = ['m','a','n','v','i']

word = functools.reduce(lambda x,y : x+y,letters)

word


# first iteration --> m + a
#sec --> ma + n
#third --> man+ v
#fourth -->manv+i

# COMMAND ----------

# reduce 

#product of all the num in the list 
from functools import reduce

list4 = [2,3,4]

sum_all = reduce((lambda x,y: x *y),list4)

sum_all

# COMMAND ----------

# python code to demonstrate working of reduce() 
  
# importing functools for reduce() 
import functools 
  
# initializing list 
lis = [1, 3, 5, 6, 2] 
  
# using reduce to compute sum of list 
print("The sum of the list elements is : ", end="") 
print(functools.reduce(lambda a, b: a+b, lis)) 
  

# COMMAND ----------

lis = [1, 3, 5, 6, 2] 

# using reduce to compute maximum element from list 
print("The maximum element of the list is : ", end="") 
print(functools.reduce(lambda a, b: a if a > b else b, lis)) 

# COMMAND ----------

# find factorial of 5

factorial = [5,4,3,2,1]

result = functools.reduce(lambda x,y : x*y, factorial)

result 


# first iteration --> 5 *4  --> 20 
#sec --> 20 * 3
#third --> 60 *2 --> 120
#fourth --> 120 * 1 --> 120 




# COMMAND ----------

def quadratic(a,b,c): 
    return lambda x : a*x**2 + b *x + c

f = quadratic(2,3,4)

f(1)

# COMMAND ----------


