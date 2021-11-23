def power_set(xs: list[str]) -> list[str]:
    if not xs:
        return ['']
    ret = []
    while xs:
        x = xs.pop()
        ret += power_set(xs)
        ret += [x+c for c in ret]
    return ret

if __name__ == "__main__":
    test = ['a', 'b', 'c']
    for s in sorted(power_set(test), key=lambda x: len(x)):
        print(s)



