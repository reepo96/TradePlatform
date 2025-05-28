
def Exact(s1, s2):
    return s1 == s2

def Left(s, count):
    return s[0:count]

def Right(s, count):
    return s[-count:]

def Text(val, precision):
    if precision == -1:
        return str(val)
    else:
        s = f'%.{precision}f'%val
        return s

def Trim(s):
    return s.strip()
