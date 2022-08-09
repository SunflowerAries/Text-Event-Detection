import csv, re
from pattern.text.en import singularize
import nltk
from nltk.corpus import stopwords

with open("main/resources/test.csv", 'r') as f, open("main/resources/refine.csv", 'w') as fa:
    cr = csv.reader(f)
    sw_nltk = stopwords.words('english')
    next(cr, None)
    for row in cr:
        date = row[8].split(' ')[0]
        id = row[10]
        keywords = row[6][1:len(row[6])-1].lower()
        keywords = list(set(filter(lambda x:x!="", re.sub(r'[^a-z]', ' ', keywords).split(" "))))
        if (len(keywords) == 0):
            continue
        keywords = list(set([singularize(keyword) for keyword in keywords]))
        keywords = list(filter (lambda keyword:len(keyword) > 2 and keyword not in sw_nltk, keywords))
        keywords = '[' + ','.join(map(str, keywords)) + ']'
        fa.write(date + '\t' + id + '\t' + keywords + '\n')