import happybase

def get_connection():
    return happybase.Connection('localhost')

def iterate_table(generator):
    try:
        while True:
            yield generator.next()
    except StopIteration:
        pass

def dump_table(generator):
    result = []
    for row in iterate_table(generator):
        result.append(row)
    return result
