import json
from tqdm import tqdm
import gc
import multiprocessing
from language_identification import LanguageIdentification
import warnings

warnings.filterwarnings("ignore")


def do_func(ids, data):
    LANGUAGE = LanguageIdentification()
    sp = []
    for line in tqdm(data):
        item = json.loads(line[:-1])
        body = item['body'].replace('\n', ' ')
        LANG = LANGUAGE.predict_lang(body)[0][0]
        if 'es' in LANG:
            sp.append(line)
        # if item['subreddit'].lower() == 'spain' or item['subreddit'].lower() == 'spanish':
        #     sp.append(line)
    return ids, sp


def get_sp(sep=10000000, processes=5, name='04'):
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
                    results.append(pool.apply_async(do_func, (i, collect)))
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
            results.append(pool.apply_async(do_func, (i, collect)))
        pool.close()
        pool.join()
        for j, res in enumerate(results):
            ids, data = res.get()
            assert j == ids
            k.extend(data)
        print(len(k))
        del data
        gc.collect()
    with open(f'sp_data/RC_2019-{name}', 'w', encoding='utf-8') as f:
        for line in k:
            f.write(line)


s = 15000000
p = 10
get_sp(sep=s, processes=p, name='01')
get_sp(sep=s, processes=p, name='02')
get_sp(sep=s, processes=p, name='03')
get_sp(sep=s, processes=p, name='04')
get_sp(sep=s, processes=p, name='05')
get_sp(sep=s, processes=p, name='06')
get_sp(sep=s, processes=p, name='07')
get_sp(sep=s, processes=p, name='08')
get_sp(sep=s, processes=p, name='09')
get_sp(sep=s, processes=p, name='10')
get_sp(sep=s, processes=p, name='11')
get_sp(sep=s, processes=p, name='12')
# get_sp(sep=20000000, processes=15, name='10')
