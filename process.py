#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from collections import defaultdict
import json
from heapq import heappop, heappush, heappushpop

HEAP_CAPACITY = 20


def main():
    counts = defaultdict(lambda: 0)

    heap = []

    with open('output.txt', 'r') as f:
        for line in f.read().splitlines():
            name, links = line.split('\t')
            counts[links] += 1

            if len(heap) < HEAP_CAPACITY:
                heappush(heap, (int(links), name))
            else:
                heappushpop(heap, (int(links), name))

            if int(links) > 200:
                print(name + ' ' + links)

    # Don't think it's worth building an iterator for this
    # Note that we *cannot* just do `most_complex = heap`, since it's
    # actually not sorted
    # ...you *could* just call `sorted` on the "heap", I guess, but
    # where's the fun in that!?
    most_complex = [heappop(heap) for _ in range(HEAP_CAPACITY)][::-1]

    data = {'counts': counts, 'most_complex': most_complex}
    with open('processed_output.txt', 'a') as f:
        f.write(json.dumps(data))


if __name__ == '__main__':
    main()
