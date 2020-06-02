###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################

'''----------------------------Work in progress-----------------------------'''
''' A function that transforms/reduces XML Gauss summaries into JSON files  '''

import os
import ast
import json
import xmltodict



def converter1(l):
    '''e.g Transforms <counter name="MCVeloHitPacker/# PackedData">50809</counter>
       into {"MCVeloHitPacker": {"PackedData": 50809}}
       Let's call this category of counters category 1'''
    dicto = {}
    for i in range(len(l)):
        s = l[i]['@name']
        n = s.find('/')
        key1 = s[:n]
        key2 = s[n + 3:]
        dicto[key1] = {key2: int(l[i]['#text'])}

    return(dicto)


def converter2(l):
    '''e.g Transforms   <counter name="TTHitMonitor/DeltaRay">1249</counter>
                        <counter name="TTHitMonitor/betaGamma">28101829</counter>
                        <counter name="TTHitMonitor/numberHits">17105</counter>
       into {"TTHitMonitor": {"betaGamma": 28101829, "DeltaRay": 1249, "numberHits": 17105}}
       Let's call this category of counters category 2'''
    dicto = dict()
    s = l[0]['@name']
    n = s.find('/')
    key1 = s[:n]
    key2 = s[n + 1:]
    dicto[key1] = {key2: int(l[0]['#text'])}
    for i in range(1, len(l)):
        s = l[i]['@name']
        n = s.find('/')
        key = s[:n]
        if key == key1:
            dicto[key1].update({s[n + 1:]: int(l[i]['#text'])})
        else:
            s = l[i]['@name']
            n = s.find('/')
            key1 = s[:n]
            key2 = s[n + 1:]
            dicto[key1] = {key2: int(l[i]['#text'])}

    return(dicto)


def converter3(l):
    '''e.g Transforms   <counter name="CheckRichOpPhot/Diff.    - Aero. Exit x">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Aero. Exit y">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Aero. Exit z">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Cherenkov Phi">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Cherenkov Theta">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Emission Point x">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Emission Point y">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Emission Point z">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Energy">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - HPD In. Point x">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - HPD In. Point y">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - HPD In. Point z">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - HPD QW Point x">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - HPD QW Point y">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - HPD QW Point z">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Parent Momentum x">38</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Parent Momentum y">46</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Parent Momentum z">-33</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Prim. Mirr. x">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Prim. Mirr. y">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Prim. Mirr. z">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Sec. Mirr. x">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Sec. Mirr. y">0</counter>
                        <counter name="CheckRichOpPhot/Diff.    - Sec. Mirr. z">0</counter>

                   into {'CheckRichOpPhot/Diff.': {'Cherenkov': {'Phi': 0, 'Theta': 0},
                                                   'Emission Point': {'x': 0, 'y': 0, 'z': 0},
                                                   'Energy': 0,
                                                   'HPD In. Point': {'x': 0, 'y': 0, 'z': 0},
                                                   'HPD QW Point': {'x': 0, 'y': 0, 'z': 0},
                                                   'Parent Momentum': {'x': 38, 'y': 46, 'z': -33},
                                                   'Prim. Mirr.': {'x': 0, 'y': 0, 'z': 0},
                                                   'Sec. Mirr.': {'x': 0, 'y': 0, 'z': 0}}}
       Let's call this category of counters category 3'''
    dicto = dict()
    s = l[0]['@name']
    n = s.find('-')
    key1 = s[:n - 1].strip()
    key2 = s[n + 2:]
    key2c = key2[:len(key2) - 2]
    key3c = key2[len(key2) - 1:]
    key2cc = key2[:len(key2) - 4]
    key3cc = key2[len(key2) - 3:]
    if key3c in ['x', 'y', 'z'] and key2 != 'Energy':
        dicto[key1] = {key2c: {key3c: int(l[0]['#text'])}}
    elif key3cc in ['Phi', 'Eta']:
        dicto[key1] = {key2cc: {key3cc: int(l[0]['#text'])}}
    else:
        dicto[key1] = {key2: int(l[0]['#text'])}
    for i in range(1, len(l)):
        s = l[i]['@name']
        n = s.find('-')
        key_1 = s[:n - 1].strip()
        key_2 = s[n + 2:]
        key_2c = key_2[:len(key_2) - 2]
        key_3c = key_2[len(key_2) - 1:]
        key_2cc = key_2[:len(key_2) - 4]
        key_3cc = key_2[len(key_2) - 3:]
        if key_2cc != key2cc:
            if key_3c in ['x', 'y', 'z'] and key_2 != 'Energy':
                dicto[key_1][key_2c] = {key_3c: int(l[i]['#text'])}
            elif key_3cc in ['Phi', 'Eta']:
                dicto[key_1] = {key_2cc: {key_3cc: int(l[i]['#text'])}}
            else:
                dicto[key_1].update({key_2: int(l[i]['#text'])})
            key2 = key_2
            key2c = key_2c
            key2cc = key_2cc
            key3cc = key_3cc
        else:
            if key_3c in ['x', 'y', 'z'] and key_2 != 'Energy':
                dicto[key_1][key_2c].update({key_3c: int(l[i]['#text'])})
            elif key_3cc in ['Phi', 'Eta']:
                dicto[key_1][key_2cc].update({key_3cc: int(l[i]['#text'])})
            else:
                dicto[key_1].update({key_2: int(l[i]['#text'])})

    return(dicto)


def ranges(l):
    ''' Returns a list containing the ranges of each category '''
    m = [l[0], l[1]]
    for i in range(2, len(l)):
        if l[i] == l[i - 1] + 1 and l[i - 1] == l[i - 2] + 1:
            m[len(m) - 1] = l[i]
        else:
            m.append(l[i])
    return m


def difisnull(d):
    ''' Returns False if a category 3 dictionnary contains a field or a subfield that has a value different from 0 ''' 
    for i in d:
        if isinstance(d[i], dict):
            for j in d[i]:
                if isinstance(d[i][j], dict):
                    for x in d[i][j]:
                        if d[i][j][x] != 0:
                            return False
                elif d[i][j] != 0:
                    return False
        elif d[i] != 0:
            return False
    return True

class XML_SUMMARY:
    def __init__(self,xmlfile):
        self.filename = xmlfile

    def xmltojson(self):
        ''' The main function that takes the name of the XMLsummary file or the path to it
            as an entry parameter and creates a JSON file with the same name in the current directory '''

        JS = dict()
        JSO = dict()

        file = open(self.filename, 'r')
        file_lines = file.readlines()
        file.close()
        file_lines = file_lines[file_lines.index('\t<counters>\n'):file_lines.index('\t</counters>\n') + 1]
        lines = [file_lines[i][1:] for i in range(len(file_lines))]
        s = ''.join(lines).replace('Theta', 'Eta')
        text_file = open("counters.xml", "w")
        n = text_file.write(s)
        text_file.close()
        with open('counters.xml') as xml_file:
            my_dict = xmltodict.parse(xml_file.read())
        xml_file.close()
        json_data = json.dumps(my_dict)
        l = my_dict['counters']['counter']
        os.remove("counters.xml")
        l_1 = []
        l_2 = []
        l_3 = []
        for i in range(len(l)):
            if l[i]['@name'].find('#') != -1 and l[i]['@name'].find('Prev') == -1 and l[i]['@name'].find('Next') == -1:
                l_1.append(i)
            if l[i]['@name'].find('/') != -1 and l[i]['@name'].find('Original') == -1 and l[i]['@name'].find('Unpacked') == -1 and l[i]['@name'].find(
                'Diff') == -1 and l[i]['@name'].find('#') == -1 and l[i]['@name'].find('Prev') == -1 and l[i]['@name'].find('Next') == -1:
                l_2.append(i)
            if l[i]['@name'].find('Diff.') != -1 and l[i]['@name'].find('Prev') == -1 and l[i]['@name'].find('Next') == -1:
                l_3.append(i)
        for i in l_1:
            JS.update(converter1(l[i:i + 1]))
        for i in range(0, len(ranges(l_2)), 2):
            JS.update(converter2(l[ranges(l_2)[i]:ranges(l_2)[i + 1] + 1]))
        for i in range(0, len(ranges(l_3)), 2):
            if not difisnull(converter3(l[ranges(l_3)[i]:ranges(l_3)[i + 1] + 1])):
                JS.update(converter3(l[ranges(l_3)[i]:ranges(l_3)[i + 1] + 1]))
      
        JSO['Counters'] = JS
        txt = str(JSO).replace('Eta', 'Theta')
        dico = ast.literal_eval(txt)
        with open(self.filename[-36:-3] + 'json', 'w') as fp:
            json.dump(dico, fp)

        return(dico)
