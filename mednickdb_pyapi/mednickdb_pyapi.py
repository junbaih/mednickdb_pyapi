import requests
import json
import datetime
import numpy
import re
import time
import dateutil.parser
import sys
import pandas as pd
from collections import OrderedDict
from operator import itemgetter
import pymongo
from sshtunnel import SSHTunnelForwarder
import pprint


param_map = {
    'fid':'id',
}

class NoImplementError(Exception):
    pass

query_kwmap = OrderedDict({
    ' and ': '&',
    ' or ': '*OR*',
    ' >= ': '=*GTE*',
    ' > ': '=*GT*',
    ' <= ': '=*LTE*',
    ' < ': '=*LT*',
    ' not in ': '=*NIN*',
    ' in ': '=*IN*',
    ' not ': '=*NE*',
    ' != ': '=*NE*',
    ' = ': '=',
    '==': '=',
    '>=': '=*GTE*',
    '>': '=*GT*',
    '<=': '=*LTE*',
    '<': '=*LT*',
    '!=': '=*NE*',
    ' & ': '&',
    ' | ': '*OR*',
    '|': '*OR*',
    '*=*':'**', #remove the extra = added after or
})


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return time.mktime(obj.timetuple())*1000  # Convert to ms since epoch
        if isinstance(obj, numpy.integer):  # TODO test
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)


class MyDecoder(json.JSONDecoder):
    def __init__(self, *args, **kargs):
        json.JSONDecoder.__init__(self, object_hook=self.parser,
                                  *args, **kargs)

    def parser(self, dct):
        for k, v in dct.items():
            if isinstance(v, str) and v == '':
                dct[k] = None
            if k in ['datemodified','dateexpired']: #TODO other dates? anything with the str "date" in it?
                dct[k] = datetime.datetime.fromtimestamp(v/1000)
            # Parse datestrings back to python datetimes
            if isinstance(v, str) and re.search('[0-9]*-[0-9]*-[0-9]*T[0-9]*:[0-9]*:', v):
                try:
                    dct[k] = dateutil.parser.parse(v)
                except:
                    pass
        return dct


def _json_loads(ret, file=False):
    try:
        ret.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(ret.headers)
        raise Exception('Server Replied "' + ret.content.decode("utf-8") + '"') from e
    if file:
        return ret.content
    else:
        return json.loads(ret.content, cls=MyDecoder)


def _parse_locals_to_data_packet(locals_dict):
    if 'self' in locals_dict:
        locals_dict.pop('self')
    if 'kwargs' in locals_dict:
        kwargs = locals_dict.pop('kwargs')
        locals_dict.update(kwargs)
    return {(param_map[k] if k in param_map else k): v for k, v in locals_dict.items() if v is not None}


class MednickAPI:
    def __init__(self, server_address, username, password, debug=False):
        """server_address address constructor"""
        self.server_address = server_address
        self.s = requests.session()
        self.username = username
        self.login_token = None
        self.debug = debug
        self.token, self.usertype = self.login(username, password)
        #headers = {'Content-Type': 'application/json'}
        #self.s.headers.update(headers)
        print('Successfully connected to server at', self.server_address, 'with', self.usertype, 'privileges')

    @staticmethod
    def format_as(ret_data, format='dataframe_single_index'):
        if format == 'nested_dict':
            return ret_data

        row_cont = []
        row_cont_dict = []
        for row in ret_data:
            if 'data' in row:
                for datatype, datadict in row.pop('data').items():
                    row.update({datatype+'.'+k: v for k, v in datadict.items()})
            row_cont_dict.append(row)
            row_cont.append(pd.Series(row))

        if format == 'flat_dict':
            return row_cont_dict

        df = pd.concat(row_cont, axis=1).T
        if format == 'dataframe_single_index':
            return df
        elif format == 'dataframe_multi_index':
            raise NotImplementedError('TODO')
        else:
            ValueError('Unknown format requested, can be single_index or multi_index')


    @staticmethod
    def extract_var(list_of_dicts, var, raise_on_missing=True):
        if raise_on_missing:
            return [d[var] for d in list_of_dicts]
        else:
            return [d[var] for d in list_of_dicts if var in d]

    @staticmethod
    def sortby(sort_x, by_key, reverse=True):
        return sorted(sort_x, key=itemgetter(by_key), reverse=reverse)

    def login(self, username, password):
        """Login to the server. Returns login token and usertype (privilages)"""
        # TODO
        # self.username = username
        # base_str = self.server_address + '/Login?' + 'Username']=username + '&Password']=password
        # ret = _json_loads(self.s.post(base_str))
        # return ret['token'], ret['usertype']
        # self.login_token = True
        return True, 'admin'

    # File related functions
    def upload_file(self, fileobject, fileformat, filetype, fileversion=None, studyid=None, versionid=None, subjectid=None,
                    visitid=None, sessionid=None):
        """Upload a file data to the filestore in the specified location. File_data should be convertable to json.
        If this is a brand new file, then add, if it exists, then overwrite. Returns file info object"""
        data_packet = _parse_locals_to_data_packet(locals())
        files = {'fileobject': data_packet.pop('fileobject')}
        if not self.debug:
            ret = self.s.post(url=self.server_address + '/files/upload', data={'data':json.dumps(data_packet, cls=MyEncoder)}, files=files)
            #ret = self.s.post(url=self.server_address, data={'data':json.dumps(data_packet, cls=MyEncoder)}, files=files)

        else:
            #req = requests.Request('POST', self.server_address+'/files/upload', data=data_packet, files=files)
            req = requests.Request('POST', self.server_address+'/files/upload', data={'data':json.dumps(data_packet, cls=MyEncoder)}, files=files)

            #req = requests.Request('POST', self.server_address ,data=data_packet, files=files)
            prep = self.s.prepare_request(req)
            print('request details:')
            print('\n'.join("%s: %s" % item for item in vars(req).items()))
            print("prep details:")
            print(prep.method, prep.headers, prep.body, sep='\n')
            #print(', '.join("%s: %s" % item for item in vars(prep).items()))
            ret = self.s.send(prep)
        #JH upload DEBUG
        print("upload status:", ret.status_code)
        #what is ops ? Need further info
        return _json_loads(ret)['ops'][0]

    def update_file_info(self, fid, **kwargs):
        """Change the location of a file on the datastore and update its info. Returns?"""

        raise NoImplementError("not implemented")
        data_packet = _parse_locals_to_data_packet(locals())
        if not self.debug:
            ret = self.s.put(url=self.server_address + '/files/update', data=data_packet)
        else:
            req = requests.Request('PUT', self.server_address+'/files/update', data=data_packet)

            #req = requests.Request('POST', self.server_address ,data=data_packet, files=files)
            prep = self.s.prepare_request(req)
            print('request details:')
            print('\n'.join("%s: %s" % item for item in vars(req).items()))
            print("prep details:")
            print(prep.method, prep.headers, prep.body, sep='\n')
            #print(', '.join("%s: %s" % item for item in vars(prep).items()))
            ret = self.s.send(prep)
        return _json_loads(ret) #TODO should return file info

    def update_parsed_status(self, fid, status):
        """Change the parsed status of a file. Status is True when parsed or False otherwise"""
        # FIXME as of 1.2.2 this function does not take status
        if not self.debug:
            ret = self.s.put(url=self.server_address + '/files/updateParsedStatus', data={'id':fid, 'status':status})
        else:
            req = requests.Request('PUT', self.server_address + '/files/updateParsedStatus',
                                   data={'id':fid, 'status':status})

            # req = requests.Request('POST', self.server_address ,data=data_packet, files=files)
            prep = self.s.prepare_request(req)
            print('request details:')
            print('\n'.join("%s: %s" % item for item in vars(req).items()))
            print("prep details:")
            print(prep.method, prep.headers, prep.body, sep='\n')
            # print(', '.join("%s: %s" % item for item in vars(prep).items()))
            ret = self.s.send(prep)
            # JH upload DEBUG
        print("upload status:", ret.status_code)
        return _json_loads(ret)

    def delete_file(self, fid, delete_all_versions=False,
                    reactivate_previous=False,
                    remove_associated_data=False):
        """Delete a file from the filestore.
            Args:
                delete_all_versions: If true, delete all version of this file
                reactivate_previous: If true, set any old versions as the active version, and trigger a reparse of these files so there data is added to the datastore
                remove_associated_data: If true, purge datastore of all data associated with this file
        """
        locals_vars = locals().copy()
        name_map = {
            'reactivate_previous': 'previous',
            'delete_all_versions': 'all',
            'remove_associated_data': 'data',
            'fid': 'id'
        }
        locals_vars.pop('self')
        data = {name_map[k]: v for k, v in locals_vars.items()}
        return _json_loads(self.s.post(self.server_address + '/files/expire', data=data))

    def get_files(self, query=None, previous_versions=False, format='nested_dict', **kwargs):
        """Retrieves a list of file info from files in the file store that match the above specifiers.
           When querying, any keys in the file profile may be included, and only matching files for all will be returned.
           Return file info's are sorted by datemodified, see format_as for return format options.
        """
        if query:
            for k, v in query_kwmap.items():
                query = query.replace(k, v)
            print(query)
            if previous_versions:
                ret = _json_loads(self.s.get(self.server_address + '/files?'+query, params={'versions': '1'}))
            else:
                ret = _json_loads(self.s.get(self.server_address + '/files?'+query))
        else:
            params = _parse_locals_to_data_packet(kwargs)
            if previous_versions:
                params.update({'versions': '1'})
            ret = _json_loads(self.s.get(self.server_address + '/files', params=params))

        ret = self.sortby(ret, 'datemodified')

        if len(ret) > 0:
            ret = self.format_as(ret, format)

        return ret

    def get_file_by_fid(self, fid):
        """Get the meta data associated with a file id (i.e. the data associated with this id in the filestore)"""
        data_packet = _parse_locals_to_data_packet(locals())
        return _json_loads(self.s.get(url=self.server_address + '/files/info', params={'id': fid}))

    def download_file(self, fid):
        """Downloads a file that matches the file id as binary data"""
        return _json_loads(self.s.get(url=self.server_address + '/files/download', params={'id': fid}), file=True)

    def download_files(self, fids):
        """Downloads a number of files from a list of file id's"""
        fids_param = '*AND*'.join(fids)
        if not self.debug:
            ret = self.s.get(url=self.server_address + '/files/downloadmultiple', params={'id': fids_param})
        else:
            req = requests.Request('GET', self.server_address + '/files/downloadmultiple',
                                   params={'id': fids_param})

            # req = requests.Request('POST', self.server_address ,data=data_packet, files=files)
            prep = self.s.prepare_request(req)
            print('request details:')
            print('\n'.join("%s: %s" % item for item in vars(req).items()))
            print("prep details:")
            print(prep.method, prep.headers, prep.body, sep='\n')
            # print(', '.join("%s: %s" % item for item in vars(prep).items()))
            ret = self.s.send(prep)
            # JH upload DEBUG
        print("upload status:", ret.status_code)
        return _json_loads(ret)

    def delete_multiple(self, fids):
        """Deletes a list of files coresponding to the given fields. Not Tested TODO"""

        raise NoImplementError("not implemented")
        fids_param = '*AND*'.join(fids)
        return _json_loads(self.s.delete(url=self.server_address + '/files/expiremultiple', data={'id': fids_param}))

    def get_deleted_files(self):
        """Retrieves a list of fileinfo for deleted files from the file store that match the above specifiers"""
        return _json_loads(self.s.get(url=self.server_address + '/files/expired'))

    def get_unparsed_files(self, previous_versions=False):
        """Return a list of fileinfo for unparsed files"""
        files = _json_loads(self.s.get(self.server_address + '/files/unparsed'))
        if not previous_versions:
            files = [file for file in files if file['active']]
        return files

    def get_parsed_files(self):
        """Return a list of fileinfos for parsed files"""
        return _json_loads(self.s.get(self.server_address + '/files/parsed'))

    def get_unique_var_values(self, var, store, **kwargs):
        """Get possible values of a variable from either data or files store.
        For example, get all filetypes for studyid=TEST from file store:
            get_unique_var_values('filetype', store='files', studyid='TEST')
        """
        if store == 'data':
            ret = self.get_data(**kwargs, format='nested_dict')
        elif store == 'files':
            ret = self.get_files(**kwargs, format='nested_dict')
        else:
            raise ValueError('Store Unknown')

        if store == 'data' and var == 'filetype':
            values = []
            for row in ret:
                values += list(row['data'].keys())
        else:
            values = []
            for row in ret:
                try:
                    values.append(row[var])
                except KeyError:
                    values.append(None)
        return list(numpy.unique(values))

    # def get_studyids(self, store="data"):
    #     """Get a list of studies stored in either the data or file store"""
    #     return _json_loads(self.s.get(self.server_address + '/' + store + '/studies'))
    #
    # def get_versionids(self, store, studyid=None, versionid=None, subjectid=None):
    #     """Get the visitids associated with a particular studyid,versionid.
    #     Either from data store (default) or file store"""
    #     params = _parse_locals_to_data_packet(locals())
    #     return _json_loads(self.s.get(self.server_address + '/' + store + '/versions', params=params))
    #
    # def get_subjectids(self, studyid=None, versionid=None):
    #     """Get a list of studies stored in either the data store"""
    #     params = _parse_locals_to_data_packet(locals())
    #     return _json_loads(self.s.get(self.server_address + '/data/subjects', params=params))
    #
    # def get_visitids(self, store, studyid=None, versionid=None, subjectid=None):
    #     """Get the visitids associated with a particular studyid,versionid.
    #     Either from data store (default) or file store"""
    #     params = _parse_locals_to_data_packet(locals())
    #     return _json_loads(self.s.get(self.server_address + '/' + store + '/visits', params=params))
    #
    # def get_sessionids(self, store, studyid=None, versionid=None, subjectid=None, visitid=None):
    #     """Get the sessionids associated with a particular studyid,versionid,visitid.
    #     Either from data store (default) or file store"""
    #     params = _parse_locals_to_data_packet(locals())
    #     return _json_loads(self.s.get(self.server_address + '/' + store + '/sessions', params=params))

    # def get_filetypes(self, store, studyid, versionid=None, subjectid=None, visitid=None, sessionid=None):
    #     """Get the filetypes associated with that level of the hierarchy from the data or file store"""
    #     _locals = locals()
    #     _locals.pop('store')
    #     params = _parse_locals_to_data_packet(_locals)
    #     if store == 'data':
    #         rows = self.get_data(format='nested_dict', **_locals)
    #         file_types = []
    #         for row in rows:
    #             file_types.append(list(row['data'].keys()))
    #         return list(numpy.unique(file_types))
    #
    #     return _json_loads(self.s.get(self.server_address + '/' + store + '/types', params=params))

    # Data Functions
    def upload_data(self, data: dict, studyid, versionid, filetype, fid, subjectid, visitid=None, sessionid=None):
        """Upload a data to the datastore in the specified location. data should be a single object of key:values and convertable to json.
        Specifiers like studyid etc contained in the data object will be extracted and used before any in the function arguments.
        If this is a new location (no data exists), then add, if it exists, merge or overwrite.
        If this data came from a particular file in the server, then please add a file id to link back to that file"""
        data_packet = _parse_locals_to_data_packet(locals())
        data_packet['sourceid'] = data_packet.pop('id')
        if not self.debug:
            response = _json_loads(self.s.post(self.server_address + '/data/upload', data={'data': json.dumps(data_packet, cls=MyEncoder)}))
        else:
            resp = self.s.post(self.server_address + '/data/upload', data={'data': json.dumps(data_packet, cls=MyEncoder)})
            print('raw content')
            print(resp.text)
            response = _json_loads(resp)
        #return response[1]['ops'][0]
        return response
    def get_data(self, query=None, discard_subsets=True, format='dataframe_single_index', **kwargs):
        """Get all the data in the datastore at the specified location. Return format as specified in args"""
        if query:
            for k, v in query_kwmap.items():
                query = query.replace(k, v)
            rows = _json_loads(self.s.get(self.server_address + '/data?'+query))
        else:
            params = _parse_locals_to_data_packet(kwargs)
            rows = _json_loads(self.s.get(self.server_address + '/data', params=params))

        if discard_subsets:
            rows = self.discard_subsets(rows)

        if len(rows) > 0:
            rows = self.format_as(rows, format=format)

        return rows

    def delete_data(self, **kwargs):
        """Delete all data at a particular level of the hierarchy or using a specific dataid given
        the data id of the data object (returned from get_data as "_id")"""
        delete_param_name = 'id'
        if delete_param_name in kwargs:
            return _json_loads(self.s.delete(self.server_address + '/data/expire', data={delete_param_name: kwargs[delete_param_name]}))
        else:
            rows = self.get_data(**kwargs, format='nested_dict', discard_subsets=False)
            for row in rows:
                self.delete_data(id=row['_id'])

    def get_data_from_single_file(self, filetype, fid, format='dataframe_single_index'):
        """ Get the data in the datastore associated with a file
        (i.e. get the data that was extracted from that file on upload)"""
        return self.get_data('data.'+filetype+'.sourceid='+fid, format=format)

    def delete_data_from_single_file(self, fid):
        """ Deletes the data in the datastore associated with a file
        (i.e. get the data that was extracted from that file on upload)"""
        if not self.debug:
            ret = self.s.delete(self.server_address + '/data/expireByFile', data={'id':fid})
        else:
            req = requests.Request('DELETE', self.server_address + '/data/expireByFile',
                                   data={'id': fid})

            # req = requests.Request('POST', self.server_address ,data=data_packet, files=files)
            prep = self.s.prepare_request(req)
            print('request details:')
            print('\n'.join("%s: %s" % item for item in vars(req).items()))
            print("prep details:")
            print(prep.method, prep.headers, prep.body, sep='\n')
            # print(', '.join("%s: %s" % item for item in vars(prep).items()))
            ret = self.s.send(prep)
            # resp = self.s.delete(self.server_address + '/data/expireByFile', data={'id':fid})
            print('raw content')
            print('  ',ret.text)
            print(ret.status_code)
            # response = _json_loads(resp)
        return _json_loads(ret)

    def delete_all_files(self, password):
        """Delete all files on the DB, use with extreme caution"""
        if password == 'nap4life': #this obviously doesn't work
            files = self.get_files()
            print(len(files), 'found, beginning delete...')
            for file in files:
                print(self.delete_file(file['_id']))
        else:
            print('Cannot delete all files on the server without correct password!')

    def discard_subsets(self, ret_data):
        hierarchical_specifiers = ['studyid', 'versionid', 'subjectid','visitid','sessionid']
        for subset_idx in range(len(ret_data)-1, -1, -1): # iterate backwards so we can drop items but dont bugger the indexes
            candidate_subset = ret_data[subset_idx]
            for superset_idx in range(len(ret_data)-1, -1, -1):
                candidate_superset = ret_data[superset_idx]
                if subset_idx == superset_idx: # compare int faster than compare dict
                    continue
                if all((k not in candidate_subset) or (candidate_subset[k] is None or candidate_subset[k] == candidate_superset[k])
                       for k in hierarchical_specifiers):
                    del ret_data[subset_idx]
                    break
        return ret_data

    def __del__(self):
        # TODO, this should trigger logout.
        pass

if __name__ == '__main__':
    #med_api = MednickAPI('https://postb.in/nRmchQgu', 'bdyetton@hotmail.com', 'Pass1234',debug=True)
    med_api = MednickAPI('http://saclab.ss.uci.edu:8000', 'bdyetton@hotmail.com', 'Pass1234',debug=True)
    print('')
    # med_api.delete_all_files(password='nap4life')
    # sys.exit()
    # med_api.delete_data(studyid='TEST')
    # med_api.delete_file(fid='5bb2788f5e52330010f10727')

    # print('update fileinfo')
    # old_file = med_api.get_files(filename='TEST_Demographics.xlsx')[0]
    # #print(old_file)
    # for k,b in old_file.items():
    #     print(k,b)
    # fid = old_file['_id']
    # old_file_format = old_file['fileformat']
    # old_file_type = old_file["filetype"]
    # old_file_ver = old_file['versionid']
    # old_file_sub_id = None#old_file['subjectid']
    # old_file_study_id = old_file['studyid']
    # # l = [old_file,fid,old_file_format,old_file_study_id,old_file_sub_id,old_file_type,old_file_ver]
    # # for i in l:
    # #     print(i)
    #
    # new_file_format = 'TestFormat'
    # new_file_type = 'TestType'
    # new_file_ver = 99
    # new_file_sub_id= 99
    # new_file_study_id = 'testFileInfoUpload'
    #
    # upd = med_api.update_file_info(fid,
    #                          fileformat=new_file_format,
    #                          filetype=new_file_type,
    #                          studyid=new_file_study_id,
    #                          subjectid=new_file_sub_id,
    #                          versionid=new_file_ver)
    # print(upd)

    # fids = med_api.get_files(studyid='TEST')
    # file_info_1 = med_api.get_file_by_fid(fid=fids[0]['_id'])
    # for k,v in file_info_1.items():
    #     print(k,v)
    # to_add = {'sessionid': 10}
    # med_api.update_file_info(fid=fids[0]['_id'], file_info=to_add)
    # file_info_1.update(to_add)
    # time.sleep(5)  # Give db 5 seconds to update
    #
    # file_info_2 = med_api.get_file_by_fid(fids[0]['_id'])
    # for k,v in file_info_2.items():
    #     print(k,v)
    # assert (file_info_2 == file_info_1)


    # print("update_parsed_status")
    # with open('testfiles/test_text_1.txt', 'r') as text1:
    #
    #     text1_info = med_api.upload_file(fileobject=text1,
    #                                fileformat='txt',
    #                                studyid='TEST',
    #                                subjectid=1,
    #                                versionid=1,
    #                                filetype='text_1')
    # unparsed = med_api.get_files(studyid='TEST')
    # unparsed_fid = set()
    # for i in unparsed:
    #     if not i['parsed']:
    #         unparsed_fid.add(i['_id'])
    # for id in unparsed_fid:
    #     med_api.update_parsed_status(id,True)
    # files = med_api.get_files(studyid='TEST')
    # for i in files:
    #     if i['_id'] in unparsed_fid:
    #         assert i['parsed']

    #
    # old_files = med_api.get_files(studyid='TEST', fileformat='txt')
    # fid_list = med_api.extract_var(old_files, "_id")
    # parsed_status_list = med_api.extract_var(old_files,"parsed")
    # # guard
    # print("fid list:")
    # print(fid_list)
    # for i in range(len(fid_list)):
    #     med_api.update_parsed_status(fid_list[i], not parsed_status_list[i])
    # time.sleep(5)
    # new_files = med_api.get_files(studyid='TEST', fileformat='txt')
    # # check if new files are retrieved in the same order as the old files
    # new_fid_list = med_api.extract_var(new_files, "_id")
    # print("new fid list:")
    # print(new_fid_list)
    # new_parsed_status_list = med_api.extract_var(new_files, "parsed")
    # # change parsed status shall not effect DB _id field
    # fid_list.sort()
    # new_fid_list.sort()
    # assert fid_list == new_fid_list
    # assert parsed_status_list == [i for i in map(lambda x:not x,new_parsed_status_list)]
    #
    # #revert
    # for i in range(len(fid_list)):
    #     med_api.update_parsed_status(fid_list[i], parsed_status_list[i])


    # fids = med_api.get_files(studyid='TEST')
    # print(fids[0]['_id'],fids[0]['parsed'])
    # print("call py method to send request")
    # med_api.update_parsed_status(fids[0]['_id'], False)
    # print("py sent update request to the server")
    # time.sleep(5)
    # fids2 = med_api.get_unparsed_files()
    # assert (fids[0] in fids2)

    # print("download multi files")
    # fids = []
    # with open('testfiles/test_text_1.txt', 'r') as text1,\
    #         open('testfiles/test_text_2.txt', 'r') as text2:
    #
    #     fids = med_api.get_files(studyid='TEST', fileformat='txt')
    #     fids.sort(key=lambda x:x['filename'])
    #     fids = med_api.extract_var(fids, '_id')
    #     print(fids)
    #     files = med_api.download_files(fids)
    #     assert fids[0] == text1.read()
    #     assert fids[1] == text2.read()

    # print("delete_multiple_and_get_delete_files")
    # files = med_api.get_files(studyid='TEST', filetype='text_1')
    # fids_to_delete = med_api.extract_var(files,"_id")
    # print(fids_to_delete)
    # med_api.delete_multiple(fids_to_delete)
    # #assert not med_api.get_files(studyid='TEST', filetype='text_1')
    # deleted_fids = med_api.extract_var(med_api.get_deleted_files(), "_id")
    # # for id in fids_to_delete:
    # #     assert id in deleted_fids
    # print("num of files deleted",len(deleted_fids))
    # deleted_fids.sort()
    #
    # print(deleted_fids[-5:])

    # print("get_unparsed_files")
    # # unparsed_files = med_api.get_files(previous_versions=True, studyid='TEST', filetype='text_1')
    # # unparsed_fids = med_api.extract_var(unparsed_files,"_id")
    # # unparsed_fids.sort()
    # # for i in unparsed_fids:
    # #     med_api.update_parsed_status(i, False)
    # retrieved_fids = med_api.extract_var(med_api.get_unparsed_files(previous_versions=True), "_id")
    # retrieved_fids.sort()
    # #assert unparsed_fids == retrieved_fids
    # print("nums of unparsed file",len(retrieved_fids))
    # print(retrieved_fids[-5:])
    #
    #med_api = mednickAPI_setup
    # with open('testfiles/test_text_1.txt', 'r') as text1,\
    #         open('testfiles/test_text_2.txt', 'r') as text2:
    #     text1_info = med_api.upload_file(fileobject=text1,
    #                                fileformat='txt',
    #                                studyid='TEST',
    #                                subjectid=1,
    #                                versionid=1,
    #                                filetype='text_1')
    #
    #     text2_info = med_api.upload_file(fileobject=text2,
    #                                fileformat='txt',
    #                                studyid='TEST',
    #                                subjectid=2,
    #                                versionid=1,
    #                                filetype='text_2')
    # vals1 = med_api.get_unique_var_values('filetype','files')
    # vals1.sort()
    # assert vals1 == ['text_1','text_2']
    # vals2 = med_api.get_unique_var_values('subjectid','files')
    # vals2.sort()
    # assert vals2 == [1,2]
    #
    # unparsed_files = med_api.get_files(previous_versions=True, studyid='TEST', filetype='text_1')
    # for i in range(len(unparsed_files)):
    #     if unparsed_files[i]['parsed']:
    #         del unparsed_files[i]
    # unparsed_fids = med_api.extract_var(unparsed_files,"_id")
    # unparsed_fids.sort()
    #
    # retrieved_fids = med_api.extract_var(med_api.get_unparsed_files(previous_versions=True), "_id")
    # retrieved_fids.sort()
    # pprint(unparsed_fids,retrieved_fids)
    # assert unparsed_fids == retrieved_fids

    #
    # print("get parsed files")
    # retrieved_fids2 = med_api.extract_var(med_api.get_parsed_files(), "_id")
    # retrieved_fids2.sort()
    # #assert unparsed_fids == retrieved_fids
    # print("nums of parsed file",len(retrieved_fids2))
    # print(retrieved_fids2[-5:])
    # pf1 = med_api.get_files(studyid='TEST')
    # for i in range(len(pf1)):
    #     if not pf1[i]['parsed']:
    #         del pf1[i]
    #
    # print(pf1)
    # pfd1 = med_api.extract_var(pf1,'_id')
    # pf2 = med_api.get_parsed_files()
    # pfd2 = med_api.extract_var(pf2,'_id')
    # print(pfd1)
    # print(pfd2)
    # assert pfd1==pfd2


    print('get unique var')
    #
    # MONGO_HOST = "saclab.ss.uci.edu"
    # MONGO_DB = "mednick"
    # MONGO_USER = ""
    # MONGO_PASS = ""
    #
    # server = SSHTunnelForwarder(
    #     MONGO_HOST,
    #     ssh_username=MONGO_USER,
    #     ssh_password=MONGO_PASS,
    #     remote_bind_address=('127.0.0.1', 27017)
    # )
    #
    # server.start()
    #
    # client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)  # server.local_bind_port is assigned local port
    # db = client[MONGO_DB]
    # table = db['fileUploads']
    # pipe = [{'$match':{'active':True,'expired':False}},{'$group':{'_id':'$filetype'}}]
    # unique_var = list(table.aggregate(pipe))
    # pprint.pprint(unique_var)
    # rlist = med_api.get_unique_var_values('filetype','files')
    # print(rlist)
    #
    # server.close()

    print('upload_data')
    # file1 = med_api.get_files(filename='scorefile1.mat')[0]
    # fid = file1['_id']
    # studyid = file1['studyid']
    # verid = file1['versionid']
    # ftype = file1['filetype']
    # subid = file1['subjectid']
    # ret = med_api.upload_data({'hello':'world','valid':False},studyid,verid,ftype,fid,subid)
    # print(ret)
    #
    #
    print('get_data')
    # d1 = med_api.get_data(format='nested_dict',studyid='TEST')
    # def recursive_search_val(d,key,val):
    #     if key in d:
    #         return d[key]==val
    #     for k in d:
    #         if type(d[k])==dict:
    #             if recursive_search_val(d[k],key,val):
    #                 return True
    #     return False
    # for i in d1:
    #     print(i)
    # dataid = med_api.extract_var(d1, '_id')
    # to_delete = dataid[0]
    # #med_api.delete_data(id=to_delete)
    # d2 = med_api.get_data(format='nested_dict',studyid='TEST')
    # print('\n')
    # for i in d2:
    #     print(i)
    # assert to_delete not in med_api.extract_var(d2,'_id')
    # assert any([recursive_search_val(i,'hello','world') for i in d1])
    # assert any([recursive_search_val(i,'valid',False) for i in d1])
    # assert not any([recursive_search_val(i,'hello1','world') for i in d1])

    # print('delete_data')
    #
    # print('get_data_from_file')
    # try:
    #     fid = med_api.get_files(filetype='sleep_scoring',filename='scorefile1.mat')[0]
    # except:
    #
    # fid = fid['_id']
    # d2 = med_api.get_data_from_single_file('sleep_scoring',fid, format='nested_dict')
    # print(d2)
    # def recursive_retrieve_val(d,key):
    #     if key in d:
    #         return d[key]
    #     v = None
    #     for k in d:
    #         if type(d[k])==dict:
    #
    #             v = recursive_retrieve_val(d[k],key)
    #             return v
    #     return v
    #assert any([recursive_retrieve_val(i,'sour')])
    def recursive_search_val(d,key,val):
        if key in d:
            return d[key]==val
        for k in d:
            if type(d[k])==dict:
                if recursive_search_val(d[k],key,val):
                    return True
        return False

    print('get_data_from_single_file')
    # file = med_api.get_files(studyid='TEST')[0]
    # #print(file)
    # fid = file['_id']
    # studyid = file['studyid']
    # verid = file['versionid']
    # ftype = file['filetype']
    # subid = file['subjectid']
    # ret = med_api.upload_data({'hello':'world','valid':False},studyid,verid,ftype,fid,subid)
    # d = med_api.get_data_from_single_file(file['filetype'],file['_id'],format='nested_dict')
    # print(d)
    # assert recursive_search_val(d[0],'hello', 'world')
    # assert recursive_search_val(d[0],'valid', False)
    #
    # t = med_api.delete_data_from_single_file(fid)
    # print(t)
    # d2= med_api.get_data_from_single_file(file['filetype'],file['_id'],format='nested_dict')
    # print(d2)


# will fail
    # print('get_non_existed_file')
    # f = med_api.get_file_by_fid('52411e421e1042001742422c')
    # print(f)
    #

    print('long file test')
    # with open('testfiles/longname.ipynb', 'rb') as longfile:
    #     fobj = longfile
    #     fformat = 'jupyternb'
    #     ftype = 'ML'
    #     med_api.upload_file(fileobject=longfile,
    #                                fileformat=fformat,
    #                                studyid='TEST',
    #                                subjectid=1,
    #                                versionid=1,
    #                                filetype=ftype)
    #     f=med_api.get_files(fileformat=fformat,
    #                                studyid='TEST',
    #                                subjectid=1,
    #                                versionid=1,
    #                                filetype=ftype)
    #     print(f)
    
    #print("upload file\n")
    # with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
    #     fids = med_api.upload_file(fileobject=uploaded_version,
    #                               fileformat='scorefile'med_api.get_unique_var_values('filetype','file'),
    #                               filetype='Yo',
    #                               studyid='TEST',
    #                               subjectid=1,
    #                               versionid=1)
    # print(fids["_id"])
    # print(fids)



    # with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
    #     fid = med_api.upload_file(fileobject=uploaded_version,
    #                               fileformat='scorefile',
    #                               filetype='Yo',
    #                               studyid='TEST',
    #                               subjectid=1,
    #                               versionid=1)

    #
    # med_api.upload_data(data={'acc': 0.2, 'std':0.1},
    #                     studyid='TEST',
    #                     subjectid=2,
    #                     versionid=1,
    #                     visitid=1,
    #                     filetype='WPA',
    #                     fid=fid)
    #
    # med_api.upload_data(data={'acc': 0.1, 'std': 0.1},
    #                     studyid='TEST',
    #                     subjectid=2,
    #                     versionid=1,
    #                     visitid=2,
    #                     filetype='WPA',
    #                     fid=fid)
    #
    # med_api.upload_data(data={'age': 22, 'sex': 'M'},
    #                     studyid='TEST',
    #                     subjectid=2,
    #                     versionid=1,
    #                     filetype='demo',
    #                     fid=fid)
    #
    #
    # #med_api.delete_data(studyid='TEST')
    # #med_api.get_unique_var_values('subjectid', 'files', studyid='TEST')
    # #b = med_api.get_data(query='studyid=TEST&data.demo.age>0', format='flat_dict')
    # #a = med_api.get_data(studyid='TEST', format='flat_dict')
    #
    #
    # sys.exit()
    # some_files = med_api.get_files()
    # print('There are', len(some_files), 'files on the server before upload')
    # print('There are', len(med_api.get_unparsed_files()), 'unparsed files before upload')
    # some_files = med_api.get_deleted_files()
    # # print('There are', len(some_files), 'deleted files on the server')
    # with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
    #     fid = med_api.upload_file(fileobject=uploaded_version,
    #                               fileformat='scorefile',
    #                               filetype='Yo',
    #                               studyid='TEST',
    #                               versionid=1)
    # print('We uploaded', len(fid), 'files')
    # #print(fid)
    # some_files = med_api.get_files()
    # print('There are', len(some_files), 'files on the server after upload')
    # print('There are', len(med_api.get_unparsed_files()), 'unparsed files after upload')
    # # print('There are', len(med_api.get_parsed_files()), 'parsed files')
    # # print('There are', med_api.get_studyids('files'), 'studies')
    # # print('There are', med_api.get_visitids('files', studyid='TEST'), 'visits in TEST')
    # print(fid[0])
    # print(med_api.get_file_by_fid(fid[0]))
    # downloaded_version = med_api.download_file(fid[0])
    # with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
    #     assert(downloaded_version == uploaded_version.read())

    print('test_case3')
    # fids = med_api.extract_var(med_api.get_files(studyid='TEST'), '_id')
    # if fids:
    #     for fid in fids:
    #         med_api.delete_file(fid, delete_all_versions=True)
    #         med_api.delete_data_from_single_file(fid)
    #     fids2 = med_api.extract_var(med_api.get_files(studyid='TEST'),'_id')
    #     assert fid not in fids2
    #     assert (fids2 == [])
    #     deleted_fids = med_api.extract_var(med_api.get_deleted_files(),'_id')
    #     assert all([dfid in deleted_fids for dfid in fids])
    # med_api.delete_data(studyid='TEST')
    # assert len(med_api.get_data(studyid='TEST', format='nested_dict')) == 0 #TODO after clearing up sourceid bug

    # fid_for_manual_upload = med_api.extract_var(med_api.get_files(studyid='TEST'), '_id')[0] # get a random fid
    # data_post = {'studyid': 'TEST',
    #              'filetype': 'MemTaskA',
    #              'data': {'accuracy': 0.9},
    #              'versionid': 1,
    #              'subjectid': 2,
    #              'visitid': 1,
    #              'sessionid': 1}
    # med_api.upload_data(**data_post, fid=fid_for_manual_upload)
