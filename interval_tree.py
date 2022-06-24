from typing import Union, List, Tuple, Dict, Optional

class Interval:
    def __init__(self, start, end) -> None:
        self.start = start
        self.end = end

    def contains(self, point) -> bool:
        return  self.start <= point <= self.end

class Node:
    def __init__(self, interval: Interval, vmax=None, vmin=None, left=None, right=None, **kwargs) -> None:
        self.interval = interval
        self.vmax = vmax if not vmax is None else self.interval.end
        self.vmin = vmin if not vmin is None else self.interval.start
        self.left = left
        self.right = right
        self.kwargs = kwargs

class IntervalTree:
    def __init__(self) -> None:
        self.root = None

    def add(self, interval: Interval, **kwargs) -> None:
        new_node = Node(interval, **kwargs)
        if self.root is None:
            self.root = new_node
            return
        n = self.root
        while True:
            n.vmax = max(n.vmax, interval.end)
            n.vmin = min(n.vmin, interval.start)
            if interval.start <= n.interval.start:
                if n.left is None:
                    n.left = new_node
                    return
                else:
                    n = n.left
            else:
                if n.right is None:
                    n.right = new_node
                    return
                else:
                    n = n.right
        return

    def query_point(self, point) -> List:
        if self.root is None:
            return []
        res = []
        queue = [self.root]
        while len(queue) > 0:
            n = queue.pop(0)
            if n.interval.start <= point <= n.interval.end:
                res.append(n.kwargs)
            if not n.left is None and n.left.vmin <= point <= n.left.vmax:
                queue.append(n.left)
            if not n.right is None and n.right.vmin <= point <= n.right.vmax:
                queue.append(n.right)
        return res

# if __name__ == '__main__':
#     import numpy as np
#     import time
#     import pandas as pd
#     from pathlib import Path
#     np.random.seed(48)

#     df = pd.read_csv(Path.home() / 'Downloads' / 'WeatherEvents_Aug16_Dec20_Publish.csv', parse_dates=['StartTime(UTC)', 'EndTime(UTC)']).sample(n=30000)

#     start = df['StartTime(UTC)']
#     end = df['EndTime(UTC)']
#     indexes = np.arange(start.size, dtype=int)

#     query = start

#     it = IntervalTree()
#     for idx, (s, e) in enumerate(zip(start, end)):
#         i = Interval(s, e)
#         it.add(i, id=idx)

#     print('interval tree generation ended')
#     start_t = time.time()
#     for q in query:
#         res = it.query_point(q)
#         # res_idx = [x['id'] for x in res]
#         # manual_res = np.stack([q >= start, q <= end], axis=-1).all(axis=-1)

#         # print(f'{res_idx}')
#         # print(indexes[manual_res])
#         # assert len(res) == manual_res.sum()

#     delta = time.time() - start_t

#     print(f'my method: {delta}')

#     other_meth = []
#     start_t = time.time()
#     for q in query:
#         for idx, (s, e) in enumerate(zip(start, end)):
#             if s <= q <= e:
#                 other_meth.append(idx)
#     delta2 = time.time() - start_t
#     print(f'other method: {delta2}')
    
