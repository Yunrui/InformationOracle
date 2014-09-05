import sys

data = sys.stdin.readlines();
for article in data:
    parts = article.split("|@@@@|");
    url = parts[1];
    time = parts[2];
    title = parts[3];
    key = "/" + parts[4][0:-1];

    if (not "\"" in title):
        print "put 'content', '" + key + "', 'm:title', \"" + title + "\"";
        print "put 'content', '" + key + "', 'm:url', '" + url + "'";
        print "put 'content', '" + key + "', 'm:time', '" + time + "'";
