from arango import ArangoClient

client = ArangoClient(hosts='http://localhost:8529')
sys_db = client.db('_system', username='root', password='secret')

if not sys_db.has_database('dynamic_tables'):
    sys_db.create_database('dynamic_tables')

db = client.db('dynamic_tables', username='root', password='secret')

if not db.has_collection('entities'):
    db.create_collection('entities')

if not db.has_collection('edges'):
    db.create_collection('edges', edge=True)

def create_entity(data):
    return db.collection('entities').insert(data)

def link_entities(from_id, to_id, label):
    return db.collection('edges').insert({
        '_from': from_id,
        '_to': to_id,
        'label': label
    })

def get_entities():
    return list(db.collection('entities').all())
