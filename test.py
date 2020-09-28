from numpy.random import uniform
import re

data = "109年09月01日"
print(re.findall(r"\d+\.?\d*", data))