import random

def parallel_shuffle(source, target, _source, _target, seed=119):
    source = open(source, 'rt')
    target = open(target, 'rt')
    data = list(zip(source, target))

    random.seed(seed)
    random.shuffle(data)

    _source = open(_source, 'wt')
    _target = open(_target, 'wt')

    for s, t in data:
        _source.write(s)
        _target.write(t)
