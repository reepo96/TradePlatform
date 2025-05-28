import math

def IntPart(x):
    if x > 0.0:
        return int(x+0.5)
    else:
        y = x-0.5
        m = int(y)
        return m

def Power(x,y):
    return math.pow(x,y)

def Sqr(x):
    return (x*x)

def Sqrt(x):
    return math.sqrt(x)