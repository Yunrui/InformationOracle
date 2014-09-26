import sys
import string
import json
import happybase
import datetime

if len(sys.argv) < 2:
    raise Error('please specify a meaningful name')
batch_name = sys.argv[1]
timestamp = long(datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f'))
magic = 100000000000000000000
colname = str(magic-timestamp)
currentUser = None
currentGroup = None
connection = happybase.Connection('localhost')
table = connection.table('user')

def commit():
    if (currentGroup != None):
        print('Commit {0}'.format(currentUser))
        print(currentGroup)
        table.put(currentUser, {'s:{0}|@@@@|{1}'.format(colname, batch_name):json.dumps(currentGroup)})

while True:
    line = sys.stdin.readline()
    if not line:
        break
    fields = [string.strip(_) for _ in string.strip(line).split('\t')]
    skip=False

    for field in fields:
        if len(field) == 0:
            skip=True
            break

    if skip:
        continue

    obj = {
        'user':fields[0],
        'id':fields[1],
        'title':fields[2],
        'url':fields[3],
        'datetime':fields[4],
        'weight':"{0:.2f}".format(float(fields[5]))
    }

    if (obj['user'] != currentUser):
        commit()
        currentUser = obj['user']
        currentGroup = []

    currentGroup.append(obj)

commit()

