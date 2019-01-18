from mednickdb_pyapi.mednickdb_pyapi import MednickAPI, NoImplementError
import pytest
import time

user = 'bdyetton@hotmail.com'
password = 'Pass1234'

server_address = 'http://saclab.ss.uci.edu:8000'

@pytest.fixture(scope="module")
def mednickAPI_setup():
    return MednickAPI(server_address, user , password)

def recursive_search_val(d,key,val):
    if key in d:
        return d[key]==val
    for k in d:
        if type(d[k])==dict:
            if recursive_search_val(d[k],key,val):
                return True
    return False


def test_login(mednickAPI_setup):
    """Test login, this will always pass until we deal with login"""
    #med_api = MednickAPI(server_address, user , password)
    med_api = mednickAPI_setup
    assert med_api.token
    assert med_api.usertype == 'admin'
'''
def test_get_nonexisted_file():
    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
    fids = med_api.get_files(studyid='SOMETHING')
    assert not len(fids)

def test_get_test_file():
    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
    fids = med_api.get_files(studyid='TEST')
    assert fids
'''

def test_clear_test_study(mednickAPI_setup):
    #med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
    med_api = mednickAPI_setup
    fids = med_api.extract_var(med_api.get_files(studyid='TEST'), '_id')
    if fids:
        for fid in fids:
            med_api.delete_file(fid, delete_all_versions=True)
            med_api.delete_data_from_single_file(fid)
        fids2 = med_api.extract_var(med_api.get_files(studyid='TEST'),'_id')
        assert fid not in fids2
        assert (fids2 == [])
        deleted_fids = med_api.extract_var(med_api.get_deleted_files(),'_id')
        assert all([dfid in deleted_fids for dfid in fids])
    med_api.delete_data(studyid='TEST')
    assert len(med_api.get_data(studyid='TEST', format='nested_dict')) == 0 #TODO after clearing up sourceid bug
    #JH modified: TEST file GET after delete
    fids3 = med_api.get_files(studyid='TEST')
    assert not len(fids3)

@pytest.mark.dependency(['test_clear_test_study'])
def test_upload_and_download_file(mednickAPI_setup):
    """Uploaded a file and download it again and make sure it matches"""
    #med_api = MednickAPI(server_address, user, password, debug=True)
    med_api = mednickAPI_setup
    files_on_server_before_upload = med_api.get_files()
    parsed_files_before_upload = med_api.get_unparsed_files()
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        file_info = med_api.upload_file(fileobject=uploaded_version,
                                  fileformat='scorefile',
                                  studyid='TEST',
                                  subjectid=1,
                                  versionid=1,
                                  filetype='sleep')
        downloaded_version = med_api.download_file(file_info['_id'])
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        assert downloaded_version == uploaded_version.read()
    files_on_server_after_upload = med_api.get_files()
    parsed_files_after_upload = med_api.get_unparsed_files()
    assert len(files_on_server_before_upload)+1 == len(files_on_server_after_upload)
    assert len(parsed_files_before_upload)+1 == len(parsed_files_after_upload)


@pytest.mark.dependency(['test_clear_test_study'])
def test_upload_and_overwrite(mednickAPI_setup):
    """Test that a file uploaded with the same name and info overwrites the older version
    When a file with the same filename, and same location in the file servers is uploaded:
        - The previous version will be set as active=False
        - The new version will get a new FID
        -

    """
    #med_api = MednickAPI(server_address, user, password)
    med_api = mednickAPI_setup
    with open('testfiles/TEST_Demographics.xlsx', 'rb') as uploaded_version_1:
        file1_info_before_overwrite = med_api.upload_file(fileobject=uploaded_version_1,
                                   fileformat='tabular',
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype='unique_thing_1')
    downloaded_version_1 = med_api.download_file(file1_info_before_overwrite['_id'])
    file_version_before_overwrite = file1_info_before_overwrite['filename_version']

    with open('testfiles/updated_versions/TEST_Demographics.xlsx', 'rb') as uploaded_version_2:
        file1_info_after_overwrite = med_api.upload_file(fileobject=uploaded_version_2,
                                   fileformat='tabular',
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype='unique_thing_1')
    downloaded_version_2 = med_api.download_file(file1_info_after_overwrite['_id'])
    file_version_after_overwrite = file1_info_after_overwrite['filename_version']

    with open('testfiles/updated_versions/TEST_Demographics.xlsx', 'rb') as uploaded_version_2:
        f = uploaded_version_2.read()
        assert downloaded_version_2 == f
        assert downloaded_version_1 != f

    #Get all versions, and make sure both versions of the file match what was uploaded
    all_versions = med_api.get_files(filename='TEST_Demographics.xlsx', previous_versions=True)
    assert all([file in med_api.extract_var(all_versions, 'filename_version') for file in [file_version_after_overwrite, file_version_before_overwrite]])

    file = med_api.get_files(filename='TEST_Demographics.xlsx')
    assert len(file) == 1
    assert file1_info_before_overwrite['_id'] != file1_info_after_overwrite['_id'] #It gets a new fid
    assert file[0]['_id'] == file1_info_after_overwrite['_id']

    downloaded_version_current = med_api.download_file(file[0]['_id'])
    assert downloaded_version_current == downloaded_version_2
    assert downloaded_version_1 != downloaded_version_2

@pytest.mark.dependency(['test_upload_and_download_file'])
def test_update_file_info(mednickAPI_setup):
    #med_api = MednickAPI(server_address, user, password)
    med_api = mednickAPI_setup
    old_file = med_api.get_files(filename='TEST_Demographics.xlsx')[0]
    fid = old_file['_id']
    old_file_format = old_file['fileformat']
    old_file_type = old_file["filetype"]
    old_file_ver = old_file['versionid']
    old_file_sub_id = old_file['subjectid']
    old_file_study_id = old_file['studyid']

    new_file_format = 'TestFormat'
    new_file_type = 'TestType'
    new_file_ver = 99
    new_file_sub_id= 99
    new_file_study_id = 'testFileInfoUpload'
    try:
        med_api.update_file_info(fid,
                                 fileformat=new_file_format,
                                 filetype=new_file_type,
                                 studyid=new_file_study_id,
                                 subjectid=new_file_sub_id,
                                 versionid=new_file_ver)
        new_file = med_api.get_files(filename='TEST_Demographics.xlsx')[0]
        new_fid = new_file['_id']

        assert new_file['fileformat'] == new_file_format
        assert new_file["filetype"] == new_file_type
        assert new_file['versionid'] == new_file_ver
        assert new_file['subjectid'] == new_file_sub_id
        assert new_file['studyid'] == new_file_study_id

    except NoImplementError:
        return
    #revert change

    # med_api.update_file_info(new_fid,
    #                          fileformat=old_file_format,
    #                          filetype=old_file_type,
    #                          studyid=old_file_study_id,
    #                          subjectid=old_file_sub_id,
    #                          versionid=old_file_ver)
    #
    # assert med_api.get_files(filename='TEST_Demographics.xlsx')[0]['fileformat'] == old_file_format
    # assert med_api.get_files(filename='TEST_Demographics.xlsx')[0]["filetype"] == old_file_type
    # assert med_api.get_files(filename='TEST_Demographics.xlsx')[0]['versionid'] == old_file_ver
    # assert med_api.get_files(filename='TEST_Demographics.xlsx')[0]['subjectid'] == old_file_sub_id
    # assert med_api.get_files(filename='TEST_Demographics.xlsx')[0]['studyid'] == old_file_study_id
    #
    # # another test case
    # fids = med_api.get_files(studyid='TEST')
    # file_info_1 = med_api.get_file_by_fid(fid=fids[0]['_id'])
    # to_add = {'sessionid': 10}
    # med_api.update_file_info(fid=fids[0]['_id'], file_info=to_add)
    # file_info_1.update(to_add)
    # time.sleep(file_update_time)  # Give db 5 seconds to update
    #
    # file_info_2 = med_api.get_file_by_fid(fids[0]['_id'])
    # assert (file_info_2 == file_info_1)

def test_update_parsed_status(mednickAPI_setup):
    #med_api = MednickAPI(server_address, user, password)
    med_api = mednickAPI_setup
    with open('testfiles/test_text_1.txt', 'r') as text1:

        text1_info = med_api.upload_file(fileobject=text1,
                                   fileformat='txt',
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype='text_1')
    unparsed = med_api.get_files(studyid='TEST')
    unparsed_fid = set()
    for i in unparsed:
        if not i['parsed']:
            unparsed_fid.add(i['_id'])
    for id in unparsed_fid:
        med_api.update_parsed_status(id,True)
    files = med_api.get_files(studyid='TEST')
    for i in files:
        if i['_id'] in unparsed_fid:
            assert i['parsed']
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
    # assert fid_list == new_fid_list
    # assert parsed_status_list == [i for i in map(lambda x:not x,new_parsed_status_list)]
    #
    # #revert
    # for i in range(len(fid_list)):
    #     med_api.update_parsed_status(fid_list[i], parsed_status_list[i])

def test_get_file_by_id(mednickAPI_setup):
    #med_api = MednickAPI(server_address, user, password)
    med_api = mednickAPI_setup
    old_files = med_api.get_files(studyid='TEST')
    old_files.sort(key = lambda x:x["_id"])

    fids = med_api.extract_var(old_files,"_id")
    fids.sort()
    for i in range(len(fids)):
        assert med_api.get_file_by_fid(fids[i])==old_files[i]

@pytest.mark.dependency(['test_upload_and_download_file'])
def test_download_files(mednickAPI_setup):
    med_api = mednickAPI_setup
    fids = []
    with open('testfiles/test_text_1.txt', 'r') as text1,\
            open('testfiles/test_text_2.txt', 'r') as text2:
        text1_info = med_api.upload_file(fileobject=text1,
                                   fileformat='txt',
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype='text_1')
        fids.append(text1_info['_id'])

        text2_info = med_api.upload_file(fileobject=text2,
                                   fileformat='txt',
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype='text_1')
        fids.append(text2_info['_id'])

        assert fids
        files = med_api.download_files(fids)
        assert files[0] == text1.read()
        assert files[1] == text2.read()






def test_delete_multiple_and_get_delete_files(mednickAPI_setup):
    med_api = mednickAPI_setup
    files = med_api.get_files(studyid='TEST', filetype='text_1')
    fids_to_delete = med_api.extract_var(files,"_id")
    try:
        med_api.delete_multiple(fids_to_delete)
        assert not med_api.get_files(studyid='TEST', filetype='text_1')
        deleted_fids = med_api.extract_var(med_api.get_deleted_files(), "_id")
        for id in fids_to_delete:
            assert id in deleted_fids
    except NoImplementError:
        return

def test_upload_data_and_get_data(mednickAPI_setup):
    med_api = mednickAPI_setup
    file1 = med_api.get_files(filename='scorefile1.mat')[0]
    fid = file1['_id']
    studyid = file1['studyid']
    verid = file1['versionid']
    ftype = file1['filetype']
    subid = file1['subjectid']
    ret = med_api.upload_data({'hello':'world','valid':False},studyid,verid,ftype,fid,subid)

    d1 = med_api.get_data(format='nested_dict',studyid='TEST')


    assert any([recursive_search_val(i,'hello','world') for i in d1])
    assert any([recursive_search_val(i,'valid',False) for i in d1])
    assert not any([recursive_search_val(i,'hello1','world') for i in d1])

#
def test_delete_data(mednickAPI_setup):
    med_api = mednickAPI_setup
    dl1 = med_api.get_data(format='nested_dict', studyid='TEST')
    data_id = med_api.extract_var(dl1, '_id')
    to_delete = data_id[0]
    med_api.delete_data(id=to_delete)
    dl2 = med_api.get_data(format='nested_dict', studyid='TEST')
    assert to_delete not in med_api.extract_var(dl2, '_id')



@pytest.mark.dependency(['test_upload_data_and_get_data'])
def test_get_and_delete_data_from_single_file(mednickAPI_setup):
    med_api = mednickAPI_setup
    file = med_api.get_files(studyid='TEST')[0]
    #print(file)
    fid = file['_id']
    studyid = file['studyid']
    verid = file['versionid']
    ftype = file['filetype']
    subid = file['subjectid']
    ret = med_api.upload_data({'hello':'world','valid':False,'point':3.5},studyid,verid,ftype,fid,subid)
    d = med_api.get_data_from_single_file(file['filetype'],file['_id'],format='nested_dict')
    #print(d)
    assert any([recursive_search_val(i,'hello', 'world') for i in d])
    assert any([recursive_search_val(i,'valid', False) for i in d])
    assert any([recursive_search_val(i, 'point',3.5) for i in d])

    med_api.delete_data_from_single_file(fid)
    assert not len(med_api.get_data_from_single_file(file['filetype'],file['_id'],format='nested_dict'))

#
# @pytest.mark.dependency(['test_get_data_from_single_file'])
# def test_delete_data_from_single_file(mednickAPI_setup):
#     med_api = mednickAPI_setup
#     file = med_api.get_files(studyid='TEST')[0]
#     #print(file)
#     fid = file['_id']
#     studyid = file['studyid']
#     verid = file['versionid']
#     ftype = file['filetype']
#     subid = file['subjectid']
#     ret = med_api.upload_data({'hello':'world','valid':False,'point':3.5},studyid,verid,ftype,fid,subid)
#
#     d = med_api.get_data_from_single_file(file['filetype'],file['_id'],format='nested_dict')

# def test_delete_all_files(mednickAPI_setup):
#    med_api = mednickAPI_setup
#    med_api.delete_all_files('')


@pytest.mark.dependency(['test_update_parsed_status'])
def test_get_parsed_files(mednickAPI_setup):
    med_api = mednickAPI_setup
    unparsed_files = med_api.get_files(studyid='TEST', filetype='text_1')
    unparsed_fids = med_api.extract_var(unparsed_files,"_id")
    for i in unparsed_fids:
        med_api.update_parsed_status(i, True)
    retrieved_fids = med_api.extract_var(med_api.get_parsed_files(), "_id")
    for i in unparsed_fids:
        assert i in retrieved_fids

    #revert change
    # for i in unparsed_fids:
    #     med_api.update_parsed_status(i, False)

def test_get_delete_files(mednickAPI_setup):
    test_clear_test_study(mednickAPI_setup)
    med_api = mednickAPI_setup
    files = med_api.get_files(studyid='TEST', filetype='text_1')
    with open('testfiles/test_text_1.txt', 'r') as text1:
        text1_info = med_api.upload_file(fileobject=text1,
                                   fileformat='txt',
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype='text_1')
        fid = text1_info['_id']
        med_api.delete_file(fid)
        fids = med_api.get_files(fileobject=text1,
                                   fileformat='txt',
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype='text_1')
        fids = med_api.extract_var(fids,'_id')
        assert fid not in fids


def test_get_unique_var_values(mednickAPI_setup):
    test_clear_test_study(mednickAPI_setup)
    med_api = mednickAPI_setup
    with open('testfiles/test_text_1.txt', 'r') as text1,\
            open('testfiles/test_text_2.txt', 'r') as text2:
        text1_info = med_api.upload_file(fileobject=text1,
                                   fileformat='txt',
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype='text_1')

        text2_info = med_api.upload_file(fileobject=text2,
                                   fileformat='txt',
                                   studyid='TEST',
                                   subjectid=2,
                                   versionid=1,
                                   filetype='text_2')
    vals1 = med_api.get_unique_var_values('filetype','files')
    vals1.sort()
    assert vals1 == ['text_1','text_2']
    vals2 = med_api.get_unique_var_values('subjectid','files')
    vals2.sort()
    assert vals2 == [1,2]

@pytest.mark.dependency(['test_update_parsed_status'])
def test_get_unparsed_files(mednickAPI_setup):
    #test_clear_test_study(mednickAPI_setup)
    med_api = mednickAPI_setup
    unparsed_files = med_api.get_files(previous_versions=True, studyid='TEST')
    toCut = []
    temp = []
    for i in range(len(unparsed_files)):
        if unparsed_files[i]['parsed']:
            print(unparsed_files[i]['_id'])
            toCut.append(i)
    for i in range(len(unparsed_files)):
        if i not in toCut:
            temp.append(unparsed_files[i])
    unparsed_files = temp
    unparsed_fids = med_api.extract_var(unparsed_files,"_id")
    unparsed_fids.sort()

    retrieved_fids = med_api.extract_var(med_api.get_unparsed_files(previous_versions=True), "_id")
    retrieved_fids.sort()
    print(unparsed_fids,"\n")
    print(retrieved_fids)
    assert unparsed_fids == retrieved_fids

def test_upload_and_get_long_name_file(mednickAPI_setup):
    med_api = mednickAPI_setup
    with open('testfiles/longname.ipynb', 'rb') as longfile:
        fformat = 'jupyternb'
        ftype = 'ML'
        med_api.upload_file(fileobject=longfile,
                                   fileformat=fformat,
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype=ftype)
        f = med_api.get_files(fileformat=fformat,
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype=ftype)
        f = f[0]
        assert fformat == f['fileformat']
        assert ftype == f['filetype']
        assert 'longname.ipynb'==f['filename'] #the one fails

@pytest.mark.dependency(['test_upload_and_overwrite'])
def test_file_query(mednickAPI_setup):
    """Upload a bunch of files to the server, and query them using all the types of querying available"""
    test_clear_test_study(mednickAPI_setup)  # Start Fresh
    #med_api = MednickAPI(server_address, user, password)
    med_api = mednickAPI_setup
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        file_info1 = med_api.upload_file(fileobject=uploaded_version,
                                  fileformat='scorefile',
                                  studyid='TEST',
                                  subjectid=1,
                                  versionid=1,
                                  filetype='sleep')
        fid1 = file_info1['_id']

    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        file_info2 = med_api.upload_file(fileobject=uploaded_version,
                                  fileformat='scorefile',
                                  studyid='TEST',
                                  subjectid=2,
                                  versionid=1,
                                  filetype='sleep')
        fid2 = file_info2['_id']

    with open('testfiles/TEST_Demographics.xlsx', 'rb') as uploaded_version_1:
        file_info3 = med_api.upload_file(fileobject=uploaded_version_1,
                                   fileformat='tabular',
                                   studyid='TEST',
                                   subjectid=3,
                                   versionid=2,
                                   filetype='unique_thing_2')
        fid3 = file_info3['_id']

    time.sleep(1)

    #Test ==
    fids = med_api.extract_var(med_api.get_files(query='studyid==TEST'),'_id')
    assert all([fid in fids for fid in [fid1, fid2, fid3]])

    #Test IN
    fids = med_api.extract_var(med_api.get_files(query='subjectid in [1,2]'),'_id')
    assert all([fid in fids for fid in [fid1, fid2]])

    #Test not in
    fids = med_api.extract_var(med_api.get_files(query='subjectid not in [1,2]'),'_id')
    assert all([fid in fids for fid in [fid3]])

    # Test and
    fids = med_api.extract_var(med_api.get_files(query='subjectid==1 and versionid==1'),'_id')
    assert all([fid in fids for fid in [fid1]])

    # Test or
    fids = med_api.extract_var(med_api.get_files(query='subjectid==2 or 1'),'_id')
    assert all([fid in fids for fid in [fid1, fid2]])

    #Test not =
    fids = med_api.extract_var(med_api.get_files(query='subjectid!=2'),'_id')
    assert all([fid in fids for fid in [fid1, fid3]])

    #Test >
    fids = med_api.extract_var(med_api.get_files(query='subjectid>2'),'_id')
    assert all([fid in fids for fid in [fid3]])

    #Test <
    fids = med_api.extract_var(med_api.get_files(query='subjectid<2'),'_id')
    assert all([fid in fids for fid in [fid1]])

    #Test <=
    fids = med_api.extract_var(med_api.get_files(query='subjectid<=2'),'_id')
    assert all([fid in fids for fid in [fid1, fid2]])

    #Test <=
    fids = med_api.extract_var(med_api.get_files(query='subjectid>=2'),'_id')
    assert all([fid in fids for fid in [fid2, fid3]])

    # Test complex #TODO
    fids = med_api.extract_var(med_api.get_files(query='subjectid>2 or <=1'),'_id')
    assert all([fid in fids for fid in [fid1, fid3]])

@pytest.mark.dependency(['test_upload_data'])
def test_data_query(mednickAPI_setup):
    #med_api = MednickAPI(server_address, user, password)
    med_api = mednickAPI_setup
    def dict_is_subset(superset, subset):
        return all(item in superset.items() for item in subset.items())

    def strip_non_matching_keys(strip_from, template):
        return {k: v for k, v in strip_from.items() if k in template}

    test_clear_test_study(mednickAPI_setup)

    with open('testfiles/TEST_Demographics.xlsx', 'rb') as uploaded_version_1:
        file_info1 = med_api.upload_file(fileobject=uploaded_version_1,
                                           fileformat='tabular',
                                           studyid='TEST',
                                           subjectid=1,
                                           versionid=2,
                                           filetype='unique_thing_3')
        fid1 = file_info1['_id']

    row1 = {'sex':'M', 'age':22, 'edu':12}
    row2 = {'sex':'F', 'age':19, 'edu':8}
    row3 = {'sex':'M', 'age':29, 'edu':18}
    med_api.upload_data(data=row1,
                        studyid='TEST',
                        subjectid=1,
                        versionid=1,
                        visitid=1,
                        filetype='demographics',
                        fid=fid1)

    med_api.upload_data(data=row2,
                        studyid='TEST',
                        subjectid=2,
                        versionid=1,
                        visitid=2,
                        filetype='demographics',
                        fid=fid1
                        )

    med_api.upload_data(data=row3,
                        studyid='TEST',
                        subjectid=3,
                        versionid=1,
                        filetype='demographics',
                        fid=fid1)

    time.sleep(1)

    #sanity check to see if we have any data at all:
    data_rows = med_api.get_data(format='nested_dict',studyid='TEST')
    assert len(data_rows) > 0

    #Test ==
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.sex==M')]
    assert all([row in data_rows for row in [row1]])

    # Test IN
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age in [22,19]')]
    assert all([row in data_rows for row in [row1, row2]])

    # Test not in
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age not in [22,19]')]
    assert all([row in data_rows for row in [row3]])

    # Test and
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age==22 and versionid==1')]
    assert all([row in data_rows for row in [row1]])

    # Test or
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age==22 or 19')]
    assert all([row in data_rows for row in [row1, row2]])

    # Test not =
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age!=22')]
    assert all([row in data_rows for row in [row2, row3]])

    # Test >
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age>19')]
    assert all([row in data_rows for row in [row1, row3]])

    # Test <
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age<22')]
    assert all([row in data_rows for row in [row2]])

    # Test <=
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age>=22')]
    assert all([row in data_rows for row in [row1, row3]])

    # Test >=
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age<=22')]
    assert all([row in data_rows for row in [row1, row2]])

    # Test complex or
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age<22 or >28')]
    assert all([row in data_rows for row in [row2, row3]])






