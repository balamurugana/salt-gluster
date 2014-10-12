# Import salt modules
import salt.client

import json
import uuid
import sys


class VolumeType:
    DISTRIBUTE = "distribute"
    REPLICATE = "replicate"


def _checkBricks(bricks, volType):
    def _getKeys(d):
        try:
            return d.keys()
        except AttributeError:
            return

    def _checkKeys(x, y):
        try:
            if len(x) == 1 and x[0] == y[0]:
                return x
        except TypeError:
            return
        else:
            return False

    if len(bricks) < 2:
        sys.stderr.write("bricks must be more than 2\n")
        return

    keys = map(_getKeys, bricks)
    if len(keys) == len(filter(lambda x: not x, keys)):
        # bricks is list
        brickList = bricks
        subVolType = None
    elif None in keys:
        sys.stderr.write("mixed type of bricks are not allowed\n")
        return
    else:
        key = reduce(_checkKeys, keys)
        if not key:
            sys.stderr.write("mixed type of bricks are not allowed\n")
            return
        values = map((lambda x: x.values()[0]), bricks)
        if filter((lambda x: isinstance(x, basestring)), values):
            sys.stderr.write("brick list is expected\n")
            return
        if filter((lambda x: len(x) < 2), values):
            sys.stderr.write("bricks must be more than 2\n")
            return
        subVolType = getattr(VolumeType, key[0].upper(), None)
        if not subVolType:
            sys.stderr.write("invalid input.  unknown subvolume type %s\n" %
                             key[0])
            return
        if subVolType == volType:
            sys.stderr.write("subvolume type can't be same volume type %s\n" %
                             volType)
            return
        brickList = sum(values, [])

    if False in map((lambda x: len(x.split(':')) == 2), brickList):
        # TODO: need to check whether splitted elements are valid or not
        sys.stderr.write("invalid brick name found\n")
        return

    if len(set(brickList)) != len(brickList):
        sys.stderr.write("A brick should be used only once\n")
        return

    # TODO: check host in brick is valid
    return True


def create(voldefstr):
    """
    voldef can be one of below

    # distribute only (simple)
    voldef = {'volume-name': "name",
              'transport': ["tcp"],
              'bricks': [
                  "node1:block",
                  "node2:block",
              ]}

    # distribute only
    voldef = {'volume-name': "name",
              'transport': ["tcp"],
              'bricks': {'distribute': [
                  "node1:block",
                  "node2:block",
              ]}}

    # replicate only
    voldef = {'volume-name': "name",
              'transport': ["tcp"],
              'bricks': {'replicate': [
                  "node1:block",
                  "node2:block",
              ]}}

    # distribute replica(s)
    voldef = {'volume-name': "name",
              'transport': ["tcp"],
              'bricks': {'distribute': [
                  {'replicate': ["node1:block", "node2:block"]},
                  {'replicate': ["node3:block", "node4:block"]},
              ]}}

    # replicate distribute(s) (unsupported)
    voldef = {'volume-name': "name",
              'transport': ["tcp"],
              'bricks': {'replicate': [
                  {'distribute': ["node1:block", "node2:block"]},
                  {'distribute': ["node3:block", "node4:block"]},
              ]}}
    """

    try:
        voldef = json.loads(voldefstr)
    except ValueError, e:
        sys.stderr.write("invalid input. %s\n" % e)
        return

    volumeName = voldef['volume-name']
    try:
        if len(voldef['bricks'].keys()) == 1:
            volType = getattr(VolumeType, voldef['bricks'].keys()[0].upper(),
                              None)
            if not volType:
                sys.stderr.write("invalid input.  unknown volume type %s" %
                                 voldef['bricks'].keys()[0])
                return
            bricks = voldef['bricks'].values()[0]
            if isinstance(bricks, basestring):
                sys.stderr.write("brick list is expected\n")
                return
        else:
            sys.stderr.write("invalid input.  "
                             "more than one brick definition found\n")
            return
    except AttributeError:
        volType = VolumeType.DISTRIBUTE
        bricks = voldef['bricks']
        if isinstance(bricks, basestring):
            sys.stderr.write("brick list is expected\n")
            return
        #voldef['bricks'] = {'distribute': voldef['bricks']}

    if not _checkBricks(bricks, volType):
        return

    # TODO:
    # 1. do pre check on host of bricks
    # 2. add uuid and other new elements to voldef
    # 3. store voldef in highly available storage
    # 4. generate volfile for bricks and volume
    # 5. push it into respective hosts
    # 6. do post check on host of bricks


if __name__ == "__main__":
    import os

    v = {"volume-name": "testvol-%d" % (os.getpid(),),
         "transport": ["tcp"],
         "bricks": {'distribute': ["h1:b1", "h1:b2"]}}

    voldefstr = json.dumps(v)
    d = json.loads(voldefstr)
    print create(voldefstr)

    v = {"volume-name": "testvol-%d" % (os.getpid(),),
         "transport": ["tcp"],
         "bricks":
         {'distribute': [
             {'replicate': ['abc:', 'a:']},
             {'replicate': ["abcd:", ":a"]},
         ]}}

    voldefstr = json.dumps(v)
    d = json.loads(voldefstr)
    print create(voldefstr)
