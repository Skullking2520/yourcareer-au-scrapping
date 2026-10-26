# YourCareer 직업·채용 정보 수집

호주 YourCareer의 직업 정보와 채용 공고를 수집하고, 직업·공고 정보를 연결해 Google Sheets에 정리하는 인턴 업무 저장소입니다.

## 수행 배경

Seeka Technology 소프트웨어 개발 인턴 업무로 진행했습니다. 인턴 기간은 2025년 1월–5월이며, 원격근무로 데이터 수집·처리 업무를 수행했습니다.

## 파일을 읽는 순서

| 단계 | 파일 | 역할 |
| --- | --- | --- |
| 직업 목록 | [occupation_scrapping.py](occupation_scrapping.py) | 직업 목록과 식별 정보 수집 |
| 직업 상세 | [occupation_detail_1.py](occupation_detail_1.py) 및 번호별 파일 | 직업별 상세 정보 수집 |
| 공고 목록 | [vacancy_scrapping_1.py](vacancy_scrapping_1.py) 및 번호별 파일 | 채용 공고 목록 수집 |
| 공고 상세 | [vacancy_detail_1.py](vacancy_detail_1.py) 및 번호별 파일 | 공고 상세 정보 수집 |
| 결과 결합 | [occ_vac_compile_1.py](occ_vac_compile_1.py) 및 번호별 파일 | 직업·공고 결과 연결 |
| 시트·작업 관리 | [google_form_package.py](google_form_package.py), [process_handler.py](process_handler.py) | 시트 연결과 진행 관리 |

번호가 붙은 파일은 동일 업무의 처리 범위를 나눈 구성입니다. 각각을 별도 프로젝트나 별도 성과로 세지 않습니다.

## 실행 준비

Python, Chrome·ChromeDriver, Google Sheets 접근 권한과 서비스 계정이 필요합니다.

```bash
python -m pip install -r requirements.txt
```

전체 실행 순서는 [workflow 기록](.github/workflows/scrapping.yaml)에 있습니다. 목록·상세·결합 단계가 시트를 통해 연결되므로, 단일 파일 실행 전 해당 단계의 입력 시트와 이전 단계 결과를 준비해야 합니다.

## 현재 자동화 설정

현재 workflow 파일은 전체가 주석 처리돼 있습니다. 파일에 기록된 일일 스케줄은 당시 실행 설정이며, 현재 수집 중이라는 의미는 아닙니다.


## 파일을 읽는 순서

| 시작 파일 | 역할 |
| --- | --- |
| [occupation_scrapping.py](<occupation_scrapping.py>) | 직업 목록 수집 |
| [occupation_detail_1.py](<occupation_detail_1.py>) | 직업 상세 수집 |
| [vacancy_scrapping_1.py](<vacancy_scrapping_1.py>) | 채용 목록 수집 |
| [vacancy_detail_1.py](<vacancy_detail_1.py>) | 채용 상세 수집 |
| [occ_vac_compile_1.py](<occ_vac_compile_1.py>) | 직업·채용 정보 결합 |

전체 파일과 분할 스크립트는 [구조 안내](docs/STRUCTURE.md)에 정리했습니다.

## 인증과 저장 대상

`SERVICE_ACCOUNT_KEY`에는 Google 서비스 계정 JSON을, `SPREADSHEET_URL`에는 본인이 관리하는 테스트 시트 URL을 환경 변수로 설정합니다. `.env.example`은 변수 목록이며 자동으로 로드되지 않습니다. 셸에서 환경 변수를 설정한 후 실행해야 합니다. 기존 업무 시트 주소와 인증 파일 저장을 제거했으므로 새 저장 대상을 먼저 지정해야 합니다.

시트 생성·초기화 함수는 기존 데이터를 지울 수 있습니다. 번호가 붙은 파일을 모두 순서대로 실행하는 방식이 아니며, 분할 구간과 시트 이름을 확인한 파일만 테스트 시트에서 실행합니다.

## 저장소 관리

실행 결과·인증 파일은 버전 관리에서 제외합니다. 공개 소스와 수집 데이터의 공개 범위는 구분하며, 인턴 업무 데이터·시트 접근 권한은 포함하지 않습니다. 보안 관련 내용은 [관리 기준](SECURITY.md)을 참고하세요.
