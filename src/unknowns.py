import sys

unks = []
with open('src/unknowns.map', 'rt') as fh:
    for line in fh:
        unks.append(line.strip().split())
unks.sort(key=lambda x: -len(x[0]))

def replace_unk(s):
    for unk in unks:
        s = s.replace(unk[0], unk[1])
    return s

if __name__ == '__main__':
    s = sys.stdin.read()
    print(replace_unk(s))
