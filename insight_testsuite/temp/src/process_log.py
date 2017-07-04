import sys
import json

import math

if len(sys.argv) < 4:
    print('missing arguments')
    exit(1)

batch_file = sys.argv[1]
stream_file = sys.argv[2]
output_file = sys.argv[3]

friends = {}  # a dictionary (map), one degree friends of given id
purchase = []  # batch and stream data record : pair(id, amount)


# bfs finding all users given layer (degree) based on root user id
def bfs(user_id, layer):
    res = {user_id}
    stack = list(friends[user_id])
    while len(stack) != 0 and layer > 0:
        new_stack = set([])
        while len(stack) != 0:
            cur = stack.pop()
            if cur not in res:
                res.add(cur)
                if cur in friends.keys():
                    new_stack = new_stack.union(friends[cur])
        stack = list(new_stack)
        layer = layer - 1
    return res


def update_new_friendship(id1, id2):
    if id1 not in friends:
        friends[id1] = {id2}
    else:
        friends[id1].add(id2)
    if id2 not in friends:
        friends[id2] = {id1}
    else:
        friends[id2].add(id1)


def update_unfriendship(id1, id2):
    friends[id1].remove(id2)
    friends[id2].remove(id1)


# for batch file
def init_purchase(user_id, amount):
    purchase.append((user_id, amount))


def convert_to_json(timestamp, id, amount, mean, sd):
    return "{\"event_type\":\"purchase\", \"timestamp\":\"" + timestamp + "\", \"id\": \"" + \
           id + "\", \"amount\": \"" + amount + "\", \"mean\": \"" + mean + "\", \"sd\": \"" + sd + "\"}";


# for streaming data
def new_purchase_detect(user_id, amount, time_stamp, D, T, of):
    global friends
    global purchase
    other_ids = bfs(user_id, D)
    other_ids.remove(user_id)
    mean = 0
    temp = []
    for item in purchase[::-1]:
        if len(temp) > T:
            break
        if item[0] in other_ids:
            temp.append(item[1])
            mean = mean + item[1]
    if len(temp) < 3:
        purchase.append((user_id, amount))
    else:
        mean = mean / float(len(temp))
        sd = sum([pow((x - mean), 2) for x in temp])
        sd = math.sqrt(sd / float(len(temp)))
        if sd * 3 + mean < amount:
            print(convert_to_json(time_stamp, str(user_id), format(amount, '.2f'), format(mean, '.2f'),
                                  format(sd, '.2f')))
            of.writelines(convert_to_json(time_stamp, str(user_id), format(amount, '.2f'), format(mean, '.2f'),
                                          format(sd, '.2f')) + '\n')
    # remove unused purchases.
    maxCheck = len(friends) * T
    if max > len(purchase):
        purchase = purchase[-maxCheck:]


def run_json_file(f, stream, D, T, of):
    for line in f:
        event = json.loads(line)
        # event type as purchase
        if event['event_type'] == 'purchase':
            user_id = int(event['id'])
            amount = float(event['amount'])
            time_stamp = event['timestamp']
            if stream:
                new_purchase_detect(user_id, amount, time_stamp, D, T, of)
            else:
                init_purchase(user_id, amount)
        # event type as befriend
        elif event['event_type'] == 'befriend':
            id1 = int(event['id1'])
            id2 = int(event['id2'])
            update_new_friendship(id1, id2)
        # event type as unfriend
        elif event['event_type'] == 'unfriend':
            id1 = int(event['id1'])
            id2 = int(event['id2'])
            update_unfriendship(id1, id2)
        else:
            print('caught error json format')
            exit(1)


bf = open(batch_file, "r")
sf = open(stream_file, "r")
of = open(output_file, "a")

params = json.loads(bf.readline())
D = int(params['D'])
T = int(params['T'])
print(D)
print(T)

# start to run
run_json_file(bf, False, D, T, of)
bf.close()
run_json_file(sf, True, D, T, of)
sf.close()
of.close()
