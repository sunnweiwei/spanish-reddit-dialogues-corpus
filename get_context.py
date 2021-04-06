import os
import json
from tqdm import tqdm


def all_file(dirname):
    _f = []
    for root, dirs, files in os.walk(dirname):
        for item in files:
            path = os.path.join(root, item)
            _f.append(path)
    return _f


data = []
for file in tqdm(all_file('sp_data')):
    data.extend([json.loads(line) for line in open(file, encoding='utf-8')])
data = [{'id': item['id'], 'parent_id': item['parent_id'], 'body': item['body']} for item in data]

id2body = {}
for item in tqdm(data):
    id2body[item['id']] = item['body']

context = []
response = []
dd = 0
mm = 0
for item in tqdm(data):
    if item['parent_id'].startswith('t1') and item['parent_id'][3:] in id2body:
        context.append(' '.join(id2body[item['parent_id'][3:]].split()))
        response.append(' '.join(item['body'].split()))
        dd += 1
    if not item['parent_id'].startswith('t1'):
        mm += 1
print(dd, len(data), mm)
print(data[0])
with open('sp_reddit/context.txt', 'w', encoding='utf-8') as f:
    for line in context:
        f.write(line + '\n')
with open('sp_reddit/response.txt', 'w', encoding='utf-8') as f:
    for line in response:
        f.write(line + '\n')
