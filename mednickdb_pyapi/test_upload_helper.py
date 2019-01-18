import pytest
import subprocess
from upload_helper import _parse_args_to_reg_ex, _file_path_to_upload_info, run_upload_helper


def test_filename_parsing():
    test_cases = {'subjectid23_visitid3.edf': ('subjectid{subjectid}_visitid{visitid}', {'subjectid': 23, 'visitid': 3}),
                  'studyidPSTIM_visitid1.edf': ('studyid{studyid}_visitid{visitid}', {'studyid': 'PSTIM', 'visitid': 1}),
                  'ID23_visit3.edf': ('ID{subjectid}_visit{visitid}', {'subjectid': 23, 'visitid': 3}),
                  'ID24_visit3.edf': ('ID{subjectid=str}_visit{visitid}', {'subjectid': '24', 'visitid': 3})}

    for file_path, (pattern, ans) in test_cases.items():
        re_exp, pattern_keys, pattern_types = _parse_args_to_reg_ex(pattern)
        file_info = _file_path_to_upload_info(file_path, re_exp, pattern_keys, pattern_types)
        assert file_info == ans

    error_test_cases = {'IDError_visit3.edf': ('ID{subjectid}_visit{visitid}', ValueError)}

    for file_path, (pattern, error) in error_test_cases.items():
        error_thrown = False
        try:
            re_exp, pattern_keys, pattern_types = _parse_args_to_reg_ex(pattern)
            file_info = _file_path_to_upload_info(file_path, re_exp, pattern_keys, pattern_types)
            assert False, "Error was not thrown"
        except error as e:
            error_thrown = True
            pass
        assert error_thrown, "Incorrect error was thrown"


def test_run_upload_helper_fail(monkeypatch):

    def input_mock(x):
        if x == "Username: ":
            return 'TEST'
        else:
            return 'y'

    monkeypatch.setattr('builtins.input', input_mock)
    monkeypatch.setattr('getpass.getpass', lambda: "test")
    error_thrown = False
    try:
        files_uploaded = run_upload_helper('testfiles/upload_helper_test_files', 'subjectid{subjectid}_visit{visitid}')
    except AssertionError:
        error_thrown = True
        pass
    assert error_thrown


def test_run_upload_helper_pass(monkeypatch):
    def input_mock(x):
        if x == "Username: ":
            return 'TEST'
        else:
            return 'y'

    monkeypatch.setattr('builtins.input', input_mock)
    monkeypatch.setattr('getpass.getpass', lambda: "TEST")

    base_specifiers = {'studyid': 'TEST',
                       'filetype': 'TEST',
                       'fileformat': 'TEST',
                       'versionid': 1}
    files_uploaded, _ = run_upload_helper('testfiles/upload_helper_test_files',
                                          'subjectid{subjectid}_visit{visitid}',
                                          {'studyid': 'TEST',
                                           'filetype': 'TEST',
                                           'fileformat': 'TEST',
                                           'versionid': 1})
    correct_files = [{'subjectid': 1, 'visitid': 1, 'filepath': 'testfiles/upload_helper_test_files\\CelliniLab_ER_Naps_subjectid1_visit1.json'},
                     {'subjectid': 4, 'visitid': 1, 'filepath': 'testfiles/upload_helper_test_files\\CelliniLab_ER_Naps_subjectid4_visit1.json'},
                     {'subjectid': 5, 'visitid': 1, 'filepath': 'testfiles/upload_helper_test_files\\CelliniLab_ER_Naps_subjectid5_visit1.json'}]

    for correct_file in correct_files:
        correct_file.update(base_specifiers)

    assert files_uploaded == correct_files