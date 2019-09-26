from xml.etree import ElementTree
from collections import namedtuple

def user_xml(path: str):
    User = namedtuple('User', ['Id', 'DisplayName', 'Location', 'CreationDate'])
    tree = ElementTree.parse(path)
    root = tree.getroot()
    data = []
    for row in root:
        entity = User(
            row.attrib.get('Id', ''),
            row.attrib.get('DisplayName', ''),
            row.attrib.get('Location', ''),
            row.attrib.get('CreationDate', '')
        )
        data.append(entity)
    return data


def post_xml(path: str):
    Post = namedtuple('Post', ['Id', 'UserId', 'Text'])
    tree = ElementTree.parse(path)
    root = tree.getroot()
    data = []
    for row in root:
        data.append(Post(
            row.attrib.get('Id'),
            row.attrib.get('UserId', ''),
            row.attrib.get('Text', '')
        ))
    return data


def comments_xml(path: str):
    Comment = namedtuple('Comment', ['Id', 'PostId', 'Score', 'UserId'])
    tree = ElementTree.parse(path)
    root = tree.getroot()
    data = []
    for row in root:
        data.append(Comment(
            row.attrib.get('Id'),
            row.attrib.get('PostId'),
            row.attrib.get('Score'),
            row.attrib.get('UserId')
        ))
    return data