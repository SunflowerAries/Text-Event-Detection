import csv, re

with open("../../data/test.csv", 'r') as f, open("../../data/refine.csv", 'w') as fa:
    cr = csv.reader(f)
    next(cr, None)
    for row in cr:
        date = row[8].split(' ')[0]
        id = row[10]
        keywords = row[6][1:len(row[6])-1].lower()
        keywords = list(set(filter(lambda x:x!="", re.sub(r'[^a-z]', ' ', keywords).split(" "))))
        if (len(keywords) == 0):
            continue
        keywords = '[' + ','.join(map(str, keywords)) + ']'
        fa.write(date + '\t' + id + '\t' + keywords + '\n')