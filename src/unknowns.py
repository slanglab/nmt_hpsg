import sys

unks = []
with open('src/unknowns.map', 'rt') as fh:
    for line in fh:
        unks.append(line.strip().split())

s = sys.stdin.read()
for unk in unks:
    s = s.replace(unk[0], unk[1])

print(s)
