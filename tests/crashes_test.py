from repository.crashes_repository import *
area = '1211'

def test_find_total_accidents_in_area():
    res = find_total_accidents_in_area(area)
    assert res > 0

def test_find_total_accidents():
    res = find_total_accidents('month', '2023-07-29', area)
    assert isinstance(res, int)
    assert res > 0

def test_find_accidents_grouped_by_cause():
    res = find_accidents_grouped_by_cause(area)
    assert isinstance(res, list)
    assert len(res) > 0
    assert isinstance(res[0], dict)

def test_extract_area_statistics():
    res = extract_area_statistics(area)
    assert isinstance(res, dict)
    assert 'injuries' in res
    assert isinstance(res['contributing_factors'], list)
    assert len(res['contributing_factors']) > 0