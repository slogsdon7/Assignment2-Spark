from pyspark import SparkContext
from operator import add
import os
import re
import preprocessing
from xml.etree import ElementTree
DATA_PATH = 'datascience.stackexchange.com'
USER_FILE = 'datascience.stackexchange.com/Users.xml'
#os.environ['JAVA_HOME'] = '/Library/Java/'

regex = r"(\w*)=\"(.*?)\""
location_regex = re.compile(r'Location=\".*\sGeorgia.*\"')


def match_location(row: str):
    location_regex = r'Location=\"(.*?)\"'
    match = re.search(location_regex, row)
    if match:
        if 'GA' in match.group(1):
            return True
        else:
            return False
    return False

def parse_xml(path: str):
    tree = ElementTree.parse(path)
    root = tree.getroot()
    data = []
    for row in root:
        user = {"Location": ""} # Empty location so we don't need to deal with a key error later
        #users.append(row.items())
        #id = row.items()[0][1]
        for k, v in row.items():
            user[k] = v
        users.append(user)
    return users






def print_user(row: str):
    user_regex = r'DisplayName=\"(.*?)\"'
    match = re.search(user_regex, row)
    print(match.group(1))

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home'
    sc: SparkContext = SparkContext.getOrCreate()
    users = preprocessing.user_xml(f'{DATA_PATH}/Users.xml')
    posts = preprocessing.post_xml(f'{DATA_PATH}/PostHistory.xml')
    post_rdd = sc.parallelize(posts)
    grouped_posts = user_rdd.groupBy(lambda s: s['PostHistoryTypeId'])
    print(grouped_posts.first())
    #counts = data.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).collect()


    sc.stop()

