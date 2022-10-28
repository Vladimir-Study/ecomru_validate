test = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
while len(test) != 0:
    if len(test) < 3:
        test = []
    print(test)
    del test[:3]
