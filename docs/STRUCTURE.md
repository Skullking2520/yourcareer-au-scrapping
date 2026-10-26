# 파일 구조 안내

직업 정보와 채용 공고를 각각 수집하고 두 정보를 결합하는 업무 코드입니다.

실행 파일 이름은 기존 작업 구간·워크플로와 연결되어 있어 유지했습니다. `docs/`에서 역할과 시작점을 찾을 수 있습니다.

| 구분 | 파일 |
| --- | --- |
| `clear.py` | [clear.py](<../clear.py>) |
| `google_form_package.py` | [google_form_package.py](<../google_form_package.py>) |
| `occ_vac_compile_*.py` | [occ_vac_compile_1.py](<../occ_vac_compile_1.py>), [occ_vac_compile_10.py](<../occ_vac_compile_10.py>), [occ_vac_compile_11.py](<../occ_vac_compile_11.py>), [occ_vac_compile_12.py](<../occ_vac_compile_12.py>), [occ_vac_compile_13.py](<../occ_vac_compile_13.py>), [occ_vac_compile_14.py](<../occ_vac_compile_14.py>), [occ_vac_compile_15.py](<../occ_vac_compile_15.py>), [occ_vac_compile_16.py](<../occ_vac_compile_16.py>), [occ_vac_compile_17.py](<../occ_vac_compile_17.py>), [occ_vac_compile_18.py](<../occ_vac_compile_18.py>), [occ_vac_compile_19.py](<../occ_vac_compile_19.py>), [occ_vac_compile_2.py](<../occ_vac_compile_2.py>), [occ_vac_compile_20.py](<../occ_vac_compile_20.py>), [occ_vac_compile_3.py](<../occ_vac_compile_3.py>), [occ_vac_compile_4.py](<../occ_vac_compile_4.py>), [occ_vac_compile_5.py](<../occ_vac_compile_5.py>), [occ_vac_compile_6.py](<../occ_vac_compile_6.py>), [occ_vac_compile_7.py](<../occ_vac_compile_7.py>), [occ_vac_compile_8.py](<../occ_vac_compile_8.py>), [occ_vac_compile_9.py](<../occ_vac_compile_9.py>) |
| `occupation_detail_*.py` | [occupation_detail_1.py](<../occupation_detail_1.py>), [occupation_detail_2.py](<../occupation_detail_2.py>), [occupation_detail_3.py](<../occupation_detail_3.py>), [occupation_detail_4.py](<../occupation_detail_4.py>), [occupation_detail_5.py](<../occupation_detail_5.py>) |
| `occupation_scrapping.py` | [occupation_scrapping.py](<../occupation_scrapping.py>) |
| `process_handler.py` | [process_handler.py](<../process_handler.py>) |
| `vacancy_detail_*.py` | [vacancy_detail_1.py](<../vacancy_detail_1.py>), [vacancy_detail_10.py](<../vacancy_detail_10.py>), [vacancy_detail_11.py](<../vacancy_detail_11.py>), [vacancy_detail_12.py](<../vacancy_detail_12.py>), [vacancy_detail_13.py](<../vacancy_detail_13.py>), [vacancy_detail_14.py](<../vacancy_detail_14.py>), [vacancy_detail_15.py](<../vacancy_detail_15.py>), [vacancy_detail_2.py](<../vacancy_detail_2.py>), [vacancy_detail_3.py](<../vacancy_detail_3.py>), [vacancy_detail_4.py](<../vacancy_detail_4.py>), [vacancy_detail_5.py](<../vacancy_detail_5.py>), [vacancy_detail_6.py](<../vacancy_detail_6.py>), [vacancy_detail_7.py](<../vacancy_detail_7.py>), [vacancy_detail_8.py](<../vacancy_detail_8.py>), [vacancy_detail_9.py](<../vacancy_detail_9.py>) |
| `vacancy_scrapping_*.py` | [vacancy_scrapping_1.py](<../vacancy_scrapping_1.py>), [vacancy_scrapping_2.py](<../vacancy_scrapping_2.py>), [vacancy_scrapping_3.py](<../vacancy_scrapping_3.py>), [vacancy_scrapping_4.py](<../vacancy_scrapping_4.py>), [vacancy_scrapping_5.py](<../vacancy_scrapping_5.py>) |

`google_form_package.py`는 Google Sheets 인증·브라우저 설정을, `process_handler.py`는 진행 상태 기록을 담당합니다.

## 실행 조건

루트 디렉터리에서 `requirements.txt`를 설치합니다. 브라우저 수집은 Chrome과 호환 드라이버가 필요하고, 시트 기록에는 테스트 시트와 쓰기 권한이 필요합니다. 외부 사이트·시트에 연결하는 실행은 문서 정리 검증 과정에서 수행하지 않았습니다.
