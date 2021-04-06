import json
from tqdm import tqdm
import gc
import multiprocessing


def do_tokenize(ids, data):
    sp = []
    for line in tqdm(data):
        item = json.loads(line[:-1])
        if item['subreddit'].lower() == 'spain' or item['subreddit'].lower() == 'spanish':
            sp.append(line)
    return ids, sp


def get_subreddit(sep=10000000, processes=5, name='04'):
    print(name)
    data = []
    k = []
    t = 0
    with open(f'raw/RC_2019-{name}', encoding='utf-8') as f:
        for line in f:
            t += 1
            if t % 1000000 == 0:
                print(t)
            data.append(line)
            if t != 0 and t % sep == 0:
                print('#', t)
                print('CPU nums', multiprocessing.cpu_count())
                pool = multiprocessing.Pool(processes=processes)
                length = len(data) // processes + 1
                results = []
                for i in range(processes):
                    collect = data[i * length:(i + 1) * length]
                    results.append(pool.apply_async(do_tokenize, (i, collect)))
                pool.close()
                pool.join()
                for j, res in enumerate(results):
                    ids, data = res.get()
                    assert j == ids
                    k.extend(data)
                print(len(k))
                del data
                gc.collect()
                data = []
        print('#', t)
        print('CPU nums', multiprocessing.cpu_count())
        pool = multiprocessing.Pool(processes=processes)
        length = len(data) // processes + 1
        results = []
        for i in range(processes):
            collect = data[i * length:(i + 1) * length]
            results.append(pool.apply_async(do_tokenize, (i, collect)))
        pool.close()
        pool.join()
        for j, res in enumerate(results):
            ids, data = res.get()
            assert j == ids
            k.extend(data)
        print(len(k))
        del data
        gc.collect()
    with open(f'subreddit/RC_2019-{name}', 'w', encoding='utf-8') as f:
        for line in k:
            f.write(line)


get_subreddit(sep=20000000, processes=5, name='04')
get_subreddit(sep=20000000, processes=5, name='10')
