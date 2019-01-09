from mednickdb_pyapi.mednickdb_pyapi import MednickAPI
import pytest
import time

user = 'bdyetton@hotmail.com'
password = 'Pass1234'

server_address = 'http://saclab.ss.uci.edu:8000'

@pytest.fixture(scope="module")
def mednickAPI_setup():
    return MednickAPI(server_address, user , password)

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

    #revert change
    med_api.update_file_info(new_fid,
                             fileformat=old_file_format,
                             filetype=old_file_type,
                             studyid=old_file_study_id,
                             subjectid=old_file_sub_id,
                             versionid=old_file_ver)

    assert med_api.get_files(filename='TEST_Demographics.xlsx')[0]['fileformat'] == old_file_format
    assert med_api.get_files(filename='TEST_Demographics.xlsx')[0]["filetype"] == old_file_type
    assert med_api.get_files(filename='TEST_Demographics.xlsx')[0]['versionid'] == old_file_ver
    assert med_api.get_files(filename='TEST_Demographics.xlsx')[0]['subjectid'] == old_file_sub_id
    assert med_api.get_files(filename='TEST_Demographics.xlsx')[0]['studyid'] == old_file_study_id

def test_update_parsed_status(mednickAPI_setup):
    #med_api = MednickAPI(server_address, user, password)
    med_api = mednickAPI_setup
    old_files = med_api.get_files(studyid='TEST')
    fid_list = med_api.extract_var(old_files, "_id")
    parsed_status_list = med_api.extract_var(old_files,"parsed")
    # guard
    assert len(fid_list) == len(parsed_status_list)
    for i in range(len(fid_list)):
        med_api.update_parsed_status(fid_list[i], not parsed_status_list[i])

    new_files = med_api.get_files(studyid='TEST')
    # check if new files are retrieved in the same order as the old files
    new_fid_list = med_api.extract_var(new_files, "_id")
    new_parsed_status_list = med_api.extract_var(new_files, "parsed")
    # change parsed status shall not effect DB _id field
    assert fid_list == new_fid_list
    assert parsed_status_list == [i for i in map(lambda x:not x,new_parsed_status_list)]

    #revert
    for i in range(len(fid_list)):
        med_api.update_parsed_status(fid_list[i], parsed_status_list[i])

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
    med_api.delete_multiple(fids_to_delete)
    assert not med_api.get_files(studyid='TEST', filetype='text_1')
    deleted_fids = med_api.extract_var(med_api.get_deleted_files(), "_id")
    for id in fids_to_delete:
        assert id in deleted_fids

#def test_get_deleted_files():
#    return
@pytest.mark.dependency(['test_update_parsed_status'])
def test_get_unparsed_files(mednickAPI_setup):
    med_api = mednickAPI_setup
    unparsed_files = med_api.get_files(previous_versions=True, studyid='TEST', filetype='text_1')
    unparsed_fids = med_api.extract_var(unparsed_files,"_id")
    unparsed_fids.sort()
    for i in unparsed_fids:
        med_api.update_parsed_status(i, False)
    retrieved_fids = med_api.extract_var(med_api.get_unparsed_files(previous_versions=True), "_id")
    retrieved_fids.sort()
    assert unparsed_fids == retrieved_fids

@pytest.mark.dependency(['test_update_parsed_status'])
def test_get_parsed_files(mednickAPI_setup):
    med_api = mednickAPI_setup
    unparsed_files = med_api.get_files(studyid='TEST', filetype='text_1')
    unparsed_fids = med_api.extract_var(unparsed_files,"_id")
    for i in unparsed_fids:
        med_api.update_parsed_status(i, True)
    retrieved_fids = med_api.extract_var(med_api.get_parsed_files(previous_versions=True), "_id")
    for i in unparsed_fids:
        assert i in retrieved_fids

    #revert change
    for i in unparsed_fids:
        med_api.update_parsed_status(i, False)

def test_get_unique_var_values():
    return

def test_upload_data():
    return

def test_get_data():
    return

def test_delete_data():
    return

def test_get_data_from_single_file():
    return

def test_delete_data_from_single_file():
    return

#def test_delete_all_files():
#    return


@pytest.mark.dependency(['test_upload_and_overwrite'])
def test_file_query():
    """Upload a bunch of files to the server, and query them using all the types of querying available"""
    test_clear_test_study()  # Start Fresh
    med_api = MednickAPI(server_address, user, password)
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
def test_data_query():
    med_api = MednickAPI(server_address, user, password)

    def dict_is_subset(superset, subset):
        return all(item in superset.items() for item in subset.items())

    def strip_non_matching_keys(strip_from, template):
        return {k: v for k, v in strip_from.items() if k in template}

    test_clear_test_study()

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






