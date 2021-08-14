d = {}
d['a'] = ["1","2","3"]
d['b'] = ["5","6"]
for key,value in d.items():
    for i in value:
        if i == "7":
            print(key)